CREATE TABLE workflow_definitions (
  id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, application_id TEXT NOT NULL, definition_key TEXT NOT NULL,
  name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
  published_revision INTEGER NOT NULL DEFAULT 0, nodes_json TEXT NOT NULL, edges_json TEXT NOT NULL,
  version BIGINT NOT NULL DEFAULT 1, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
  created_by TEXT NOT NULL, updated_by TEXT NOT NULL, UNIQUE (tenant_id,definition_key)
);
CREATE INDEX idx_workflow_definitions_list ON workflow_definitions(tenant_id,application_id,status,updated_at DESC,id);
CREATE TABLE workflow_definition_revisions (
  definition_id TEXT NOT NULL REFERENCES workflow_definitions(id), revision INTEGER NOT NULL,
  tenant_id TEXT NOT NULL, application_id TEXT NOT NULL, definition_key TEXT NOT NULL, name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '', nodes_json TEXT NOT NULL, edges_json TEXT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL, version BIGINT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, created_by TEXT NOT NULL, updated_by TEXT NOT NULL,
  PRIMARY KEY(definition_id,revision)
);
CREATE INDEX idx_workflow_revisions_tenant ON workflow_definition_revisions(tenant_id,definition_key,revision DESC);
CREATE TABLE workflow_instances (
  id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, definition_id TEXT NOT NULL REFERENCES workflow_definitions(id),
  definition_revision INTEGER NOT NULL, business_key TEXT NOT NULL, idempotency_key TEXT NOT NULL, title TEXT NOT NULL,
  starter_id TEXT NOT NULL, status TEXT NOT NULL, current_node_id TEXT NOT NULL DEFAULT '', variables_json TEXT NOT NULL,
  result_json TEXT NOT NULL DEFAULT '{}', error_code TEXT NOT NULL DEFAULT '', error_message TEXT NOT NULL DEFAULT '',
  temporal_workflow_id TEXT NOT NULL, temporal_run_id TEXT NOT NULL DEFAULT '', started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ NULL, version BIGINT NOT NULL DEFAULT 1, created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL, created_by TEXT NOT NULL, updated_by TEXT NOT NULL,
  UNIQUE (tenant_id,idempotency_key), UNIQUE (tenant_id,definition_id,business_key)
);
CREATE INDEX idx_workflow_instances_list ON workflow_instances(tenant_id,status,started_at DESC,id);
CREATE INDEX idx_workflow_instances_definition ON workflow_instances(tenant_id,definition_id,started_at DESC,id);
CREATE TABLE workflow_tasks (
  id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, instance_id TEXT NOT NULL REFERENCES workflow_instances(id),
  node_id TEXT NOT NULL, name TEXT NOT NULL, assignee_type TEXT NOT NULL, assignee TEXT NOT NULL,
  claimed_by TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, decision TEXT NOT NULL DEFAULT '', comment TEXT NOT NULL DEFAULT '',
  input_json TEXT NOT NULL DEFAULT '{}', output_json TEXT NOT NULL DEFAULT '{}', due_at TIMESTAMPTZ NULL,
  completed_at TIMESTAMPTZ NULL, version BIGINT NOT NULL DEFAULT 1, created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL, created_by TEXT NOT NULL, updated_by TEXT NOT NULL, UNIQUE(instance_id,node_id)
);
CREATE INDEX idx_workflow_tasks_inbox ON workflow_tasks(tenant_id,status,assignee_type,assignee,created_at DESC,id);
CREATE INDEX idx_workflow_tasks_instance ON workflow_tasks(tenant_id,instance_id,created_at,id);
CREATE TABLE workflow_task_history (
  id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, task_id TEXT NOT NULL REFERENCES workflow_tasks(id), instance_id TEXT NOT NULL,
  action TEXT NOT NULL, actor_id TEXT NOT NULL, from_status TEXT NOT NULL, to_status TEXT NOT NULL,
  detail_json TEXT NOT NULL DEFAULT '{}', version BIGINT NOT NULL DEFAULT 1, created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL, created_by TEXT NOT NULL, updated_by TEXT NOT NULL
);
CREATE INDEX idx_workflow_task_history_task ON workflow_task_history(tenant_id,task_id,created_at,id);
CREATE TABLE workflow_outbox_events (
  id TEXT PRIMARY KEY, subject TEXT NOT NULL, envelope BYTEA NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMPTZ NOT NULL, published_at TIMESTAMPTZ NULL, last_error TEXT NOT NULL DEFAULT '',
  version BIGINT NOT NULL DEFAULT 1, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
  created_by TEXT NOT NULL, updated_by TEXT NOT NULL
);
CREATE INDEX idx_workflow_outbox_pending ON workflow_outbox_events(published_at,available_at,created_at);
