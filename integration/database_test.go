//go:build integration

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	platformoutbox "github.com/lihongjie0209/microservice-platform-go/outbox"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/workflow-service/internal/config"
	appdb "github.com/lihongjie0209/workflow-service/internal/database"
	"github.com/lihongjie0209/workflow-service/internal/migration"
	"github.com/lihongjie0209/workflow-service/internal/workflow"
	"github.com/lihongjie0209/workflow-service/internal/workflowevent"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestRepositoryAndMigrations(t *testing.T) {
	for _, databaseType := range []string{"postgres", "mysql"} {
		t.Run(databaseType, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			dsn, migrationURL := startDatabase(t, ctx, databaseType)
			migrationPath, err := filepath.Abs(filepath.Join("..", "migrations", databaseType))
			if err != nil {
				t.Fatal(err)
			}
			schema := ""
			if databaseType == "postgres" {
				schema = "integration_postgres"
			}
			migrationCfg := config.Migration{Path: migrationPath, DatabaseURL: migrationURL, Table: "integration_" + databaseType + "_schema_migrations", Schema: schema, CreateSchema: schema != ""}
			migrationErrors := make(chan error, 3)
			var migrations sync.WaitGroup
			for range 3 {
				migrations.Add(1)
				go func() {
					defer migrations.Done()
					migrationErrors <- migration.Run(migrationCfg, "up", 0)
				}()
			}
			migrations.Wait()
			close(migrationErrors)
			for err := range migrationErrors {
				if err != nil {
					t.Fatalf("concurrent migration up: %v", err)
				}
			}

			db, err := appdb.Open(ctx, config.Database{Type: databaseType, DSN: dsn, Schema: schema, MaxOpenConns: 5, MaxIdleConns: 2, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, PingTimeout: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			var workflowTables int
			if databaseType == "postgres" {
				if err := db.GetContext(ctx, &workflowTables, `SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename = 'workflow_instances'`); err != nil {
					t.Fatal(err)
				}
				var timezone string
				if err := db.GetContext(ctx, &timezone, `SHOW TIMEZONE`); err != nil || timezone != "Asia/Shanghai" {
					t.Fatalf("timezone=%q err=%v", timezone, err)
				}
			} else if err := db.GetContext(ctx, &workflowTables, `SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'workflow_instances'`); err != nil {
				t.Fatal(err)
			}
			if workflowTables != 1 {
				t.Fatalf("workflow_instances tables = %d", workflowTables)
			}

			outboxStore, err := platformoutbox.NewSQLStore(db, "workflow_outbox_events")
			if err != nil {
				t.Fatal(err)
			}
			repository, err := workflow.NewRepository(db, outboxStore, workflowevent.NewFactory())
			if err != nil {
				t.Fatal(err)
			}
			requestCtx := platformprincipal.WithContext(ctx, platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
			now := time.Now()
			nodes := []workflow.Node{{ID: "start", Name: "Start", Type: workflow.NodeStart}, {ID: "approval", Name: "Approve", Type: workflow.NodeApproval, AssigneeType: workflow.AssigneeRole, Assignee: "approver", TimeoutSeconds: 3600}, {ID: "end", Name: "End", Type: workflow.NodeEnd}}
			edges := []workflow.Edge{{FromNodeID: "start", ToNodeID: "approval"}, {FromNodeID: "approval", ToNodeID: "end"}}
			definition, err := repository.CreateDefinition(requestCtx, workflow.Definition{ID: "definition-1", TenantID: "tenant-1", ApplicationID: "application-1", Key: "leave.approval", Name: "Leave", Status: workflow.DefinitionDraft, NodesJSON: `[{"id":"start","name":"Start","type":"start"},{"id":"approval","name":"Approve","type":"approval","assignee_type":"role","assignee":"approver","timeout_seconds":3600},{"id":"end","name":"End","type":"end"}]`, EdgesJSON: `[{"from_node_id":"start","to_node_id":"approval","priority":0},{"from_node_id":"approval","to_node_id":"end","priority":0}]`, Nodes: nodes, Edges: edges, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: "user-1", UpdatedBy: "user-1"})
			if err != nil {
				t.Fatalf("CreateDefinition: %v", err)
			}
			definition, err = repository.PublishDefinition(requestCtx, "tenant-1", definition.ID, definition.Version, "user-1")
			if err != nil {
				t.Fatalf("PublishDefinition: %v", err)
			}
			instance, err := repository.CreateInstance(requestCtx, workflow.Instance{ID: "instance-1", TenantID: "tenant-1", DefinitionID: definition.ID, DefinitionRevision: definition.PublishedRevision, BusinessKey: "leave-1", IdempotencyKey: "request-0001", Title: "Leave", StarterID: "user-1", Status: workflow.InstanceRunning, VariablesJSON: `{}`, ResultJSON: `{}`, TemporalWorkflowID: "workflow:tenant-1:instance-1", StartedAt: now, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: "user-1", UpdatedBy: "user-1"}, definition)
			if err != nil {
				t.Fatalf("CreateInstance: %v", err)
			}
			task, err := repository.CreateTask(platformprincipal.SystemContext(ctx, "workflow-worker"), workflow.Task{ID: "task-1", TenantID: "tenant-1", InstanceID: instance.ID, NodeID: "approval", Name: "Approve", AssigneeType: workflow.AssigneeRole, Assignee: "approver", Status: workflow.TaskPending, InputJSON: `{}`, OutputJSON: `{}`, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: "workflow-worker", UpdatedBy: "workflow-worker"})
			if err != nil {
				t.Fatalf("CreateTask: %v", err)
			}
			task, err = repository.CompleteTask(requestCtx, task, workflow.DecisionApprove, "ok", `{"approved":true}`, "user-1")
			if err != nil {
				t.Fatalf("CompleteTask: %v", err)
			}
			if task.Status != workflow.TaskApproved || task.Version != 2 {
				t.Fatalf("task = %#v", task)
			}
			var eventCount, historyCount int
			if err := db.GetContext(ctx, &eventCount, `SELECT count(*) FROM workflow_outbox_events`); err != nil {
				t.Fatal(err)
			}
			if err := db.GetContext(ctx, &historyCount, `SELECT count(*) FROM workflow_task_history`); err != nil {
				t.Fatal(err)
			}
			if eventCount != 4 || historyCount != 1 {
				t.Fatalf("events=%d history=%d", eventCount, historyCount)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := migration.Run(migrationCfg, "down", 0); err != nil {
				t.Fatalf("migration down: %v", err)
			}
		})
	}
}

func startDatabase(t *testing.T, ctx context.Context, databaseType string) (string, string) {
	t.Helper()
	switch databaseType {
	case "postgres":
		container, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("app"), postgres.BasicWaitStrategies(), postgres.WithSQLDriver("pgx"))
		if err != nil {
			t.Fatal(err)
		}
		testcontainers.CleanupContainer(t, container)
		dsn, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		return dsn, dsn
	case "mysql":
		container, err := mysql.Run(ctx, "mysql:8.4", mysql.WithDatabase("app"), mysql.WithUsername("app"), mysql.WithPassword("app"))
		if err != nil {
			t.Fatal(err)
		}
		testcontainers.CleanupContainer(t, container)
		dsn, err := container.ConnectionString(ctx, "parseTime=true")
		if err != nil {
			t.Fatal(err)
		}
		migrationDSN, err := container.ConnectionString(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return dsn, "mysql://" + migrationDSN
	default:
		t.Fatal(fmt.Errorf("unsupported database %q", databaseType))
		return "", ""
	}
}
