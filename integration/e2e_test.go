//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	authorizationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/authorization/v1"
	workflowv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/workflow/v1"
	"github.com/lihongjie0209/workflow-service/internal/app"
	"github.com/lihongjie0209/workflow-service/internal/auth"
	"github.com/lihongjie0209/workflow-service/internal/config"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func TestHTTPJWTAndGRPCWorkflowEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	postgresContainer, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("app"), postgres.BasicWaitStrategies(), postgres.WithSQLDriver("pgx"))
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, postgresContainer)
	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	migrationPath, err := filepath.Abs(filepath.Join("..", "migrations", "postgres"))
	if err != nil {
		t.Fatal(err)
	}
	authorizationAddress := freeAddress(t)
	authorizationListener, err := net.Listen("tcp", authorizationAddress)
	if err != nil {
		t.Fatal(err)
	}
	authorizationServer := grpc.NewServer()
	authorizationv1.RegisterAuthorizationServiceServer(authorizationServer, &authorizationStub{})
	go func() { _ = authorizationServer.Serve(authorizationListener) }()
	t.Cleanup(authorizationServer.Stop)

	httpAddress, grpcAddress := freeAddress(t), freeAddress(t)
	const secret = "01234567890123456789012345678901"
	cfg := config.Config{
		Runtime: config.Runtime{ActiveProfile: "integration"}, App: config.App{Name: "workflow-service", Env: "integration", ShutdownTimeout: 10 * time.Second},
		HTTP:      config.HTTP{Address: httpAddress, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, RequestTimeout: 5 * time.Second, MaxBodyBytes: 1 << 20},
		GRPC:      config.GRPC{Enabled: true, Address: grpcAddress, MaxReceiveBytes: 4 << 20},
		Log:       config.Log{Level: "error", Format: "json", File: filepath.Join(t.TempDir(), "app.log"), MaxSizeMB: 1, MaxBackups: 1, MaxAgeDays: 1},
		Database:  config.Database{Enabled: true, Name: "app", Schema: "workflow_e2e", Type: "postgres", DSN: dsn, MaxOpenConns: 5, MaxIdleConns: 2, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, PingTimeout: 10 * time.Second},
		Migration: config.Migration{AutoUp: true, CreateSchema: true, Path: migrationPath, DatabaseURL: dsn, Table: "workflow_e2e_schema_migrations", Schema: "workflow_e2e", DatabaseName: "app"},
		Health:    config.Health{DatabaseTimeout: 2 * time.Second, RedisTimeout: 2 * time.Second}, Observability: config.Observability{MetricsEnabled: false},
		JWT: config.JWT{Issuer: "integration", Secret: secret, TTL: time.Hour}, Auth: config.Auth{ClientID: "client", ClientSecret: "secret", SkipHTTPPaths: []string{"/api/v1/version"}, SkipGRPCMethods: []string{"/grpc.health.v1.Health/*"}, PSK: config.PSK{Enabled: true, Key: secret, HTTPPaths: []string{"/api/v1/workflow/definitions/get"}, GRPCMethods: []string{"/platform.workflow.v1.WorkflowService/GetDefinition"}}},
		Outbound: config.Outbound{GRPC: map[string]config.GRPCUpstream{"authorization": {Target: authorizationAddress, Timeout: 5 * time.Second, Retry: config.Retry{MaxAttempts: 1, InitialBackoff: 10 * time.Millisecond, MaxBackoff: time.Second}}}},
	}
	application := app.New(cfg)
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = application.Stop(stopCtx)
	})
	token, err := auth.New(cfg).Issue("user-1")
	if err != nil {
		t.Fatal(err)
	}
	baseURL := "http://" + httpAddress
	versionResponse := postJSONBody(t, baseURL+"/api/v1/version", "", `{}`)
	if versionResponse["code"].(float64) != 0 {
		t.Fatalf("version response = %#v", versionResponse)
	}
	createBody := `{"tenant_id":"tenant-1","application_id":"app-1","key":"leave.approval","name":"Leave approval","nodes":[{"id":"start","name":"Start","type":"start"},{"id":"approval","name":"Approve","type":"approval","assignee_type":"role","assignee":"role-approver","timeout_seconds":3600},{"id":"end","name":"End","type":"end"}],"edges":[{"from_node_id":"start","to_node_id":"approval"},{"from_node_id":"approval","to_node_id":"end"}]}`
	createResponse := postJSONBody(t, baseURL+"/api/v1/workflow/definitions/create", "Bearer "+token, createBody)
	body := createResponse["body"].(map[string]any)
	definitionID := body["id"].(string)
	if createResponse["code"].(float64) != 0 || definitionID == "" {
		t.Fatalf("create response = %#v", createResponse)
	}
	postJSONBody(t, baseURL+"/api/v1/workflow/definitions/publish", "Bearer "+token, `{"tenant_id":"tenant-1","id":"`+definitionID+`","expected_version":1}`)
	pskDefinition := postJSONBody(t, baseURL+"/api/v1/workflow/definitions/get", "PSK "+secret, `{"tenant_id":"tenant-1","id":"`+definitionID+`"}`)
	if pskDefinition["code"].(float64) != 0 {
		t.Fatalf("PSK definition response = %#v", pskDefinition)
	}
	startResponse := postJSONBody(t, baseURL+"/api/v1/workflow/instances/start", "Bearer "+token, `{"tenant_id":"tenant-1","definition_key":"leave.approval","business_key":"leave-1","title":"Leave","variables_json":"{}","idempotency_key":"request-0001"}`)
	if startResponse["code"].(float64) != 0 {
		t.Fatalf("start response = %#v", startResponse)
	}

	connection, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	healthResponse, err := grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil || healthResponse.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("health = %#v, %v", healthResponse, err)
	}
	grpcCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "PSK "+secret)
	definitionResponse, err := workflowv1.NewWorkflowServiceClient(connection).GetDefinition(grpcCtx, &workflowv1.GetDefinitionRequest{TenantId: "tenant-1", Id: definitionID})
	if err != nil || definitionResponse.GetDefinition().GetPublishedRevision() != 1 {
		t.Fatalf("GetDefinition = %#v, %v", definitionResponse, err)
	}
}

type authorizationStub struct {
	authorizationv1.UnimplementedAuthorizationServiceServer
}

func (*authorizationStub) ListBindings(context.Context, *authorizationv1.ListBindingsRequest) (*authorizationv1.ListBindingsResponse, error) {
	return &authorizationv1.ListBindingsResponse{}, nil
}
func (*authorizationStub) Check(context.Context, *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
	return &authorizationv1.CheckResponse{Allowed: true}, nil
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
func postJSONBody(t *testing.T, target, authorization, body string) map[string]any {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, target, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", authorization)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("decode response: %v (%s)", err, data)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d response=%s", response.StatusCode, data)
	}
	return result
}
