CREATE TABLE workflow_definitions (
  id VARCHAR(36) PRIMARY KEY, tenant_id VARCHAR(255) NOT NULL, application_id VARCHAR(255) NOT NULL,
  definition_key VARCHAR(128) NOT NULL, name VARCHAR(255) NOT NULL, description TEXT NOT NULL,
  status VARCHAR(32) NOT NULL, published_revision INTEGER NOT NULL DEFAULT 0, nodes_json LONGTEXT NOT NULL,
  edges_json LONGTEXT NOT NULL, version BIGINT NOT NULL DEFAULT 1, created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL, created_by VARCHAR(255) NOT NULL, updated_by VARCHAR(255) NOT NULL,
  UNIQUE KEY uq_workflow_definition_key (tenant_id,definition_key),
  INDEX idx_workflow_definitions_list (tenant_id,application_id,status,updated_at,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE workflow_definition_revisions (
  definition_id VARCHAR(36) NOT NULL, revision INTEGER NOT NULL, tenant_id VARCHAR(255) NOT NULL,
  application_id VARCHAR(255) NOT NULL, definition_key VARCHAR(128) NOT NULL, name VARCHAR(255) NOT NULL,
  description TEXT NOT NULL, nodes_json LONGTEXT NOT NULL, edges_json LONGTEXT NOT NULL,
  published_at DATETIME(6) NOT NULL, version BIGINT NOT NULL DEFAULT 1, created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL, created_by VARCHAR(255) NOT NULL, updated_by VARCHAR(255) NOT NULL,
  PRIMARY KEY(definition_id,revision), INDEX idx_workflow_revisions_tenant (tenant_id,definition_key,revision),
  CONSTRAINT fk_workflow_revision_definition FOREIGN KEY (definition_id) REFERENCES workflow_definitions(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE workflow_instances (
  id VARCHAR(36) PRIMARY KEY, tenant_id VARCHAR(255) NOT NULL, definition_id VARCHAR(36) NOT NULL,
  definition_revision INTEGER NOT NULL, business_key VARCHAR(255) NOT NULL, idempotency_key VARCHAR(128) NOT NULL,
  title VARCHAR(255) NOT NULL, starter_id VARCHAR(255) NOT NULL, status VARCHAR(32) NOT NULL,
  current_node_id VARCHAR(128) NOT NULL, variables_json LONGTEXT NOT NULL, result_json LONGTEXT NOT NULL,
  error_code VARCHAR(128) NOT NULL, error_message TEXT NOT NULL, temporal_workflow_id VARCHAR(255) NOT NULL,
  temporal_run_id VARCHAR(255) NOT NULL, started_at DATETIME(6) NOT NULL, finished_at DATETIME(6) NULL,
  version BIGINT NOT NULL DEFAULT 1, created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
  created_by VARCHAR(255) NOT NULL, updated_by VARCHAR(255) NOT NULL,
  UNIQUE KEY uq_workflow_instance_idempotency (tenant_id,idempotency_key),
  UNIQUE KEY uq_workflow_instance_business (tenant_id,definition_id,business_key),
  INDEX idx_workflow_instances_list (tenant_id,status,started_at,id),
  INDEX idx_workflow_instances_definition (tenant_id,definition_id,started_at,id),
  CONSTRAINT fk_workflow_instance_definition FOREIGN KEY (definition_id) REFERENCES workflow_definitions(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE workflow_tasks (
  id VARCHAR(36) PRIMARY KEY, tenant_id VARCHAR(255) NOT NULL, instance_id VARCHAR(36) NOT NULL,
  node_id VARCHAR(128) NOT NULL, name VARCHAR(255) NOT NULL, assignee_type VARCHAR(32) NOT NULL,
  assignee VARCHAR(255) NOT NULL, claimed_by VARCHAR(255) NOT NULL, status VARCHAR(32) NOT NULL,
  decision VARCHAR(32) NOT NULL, comment TEXT NOT NULL, input_json LONGTEXT NOT NULL, output_json LONGTEXT NOT NULL,
  due_at DATETIME(6) NULL, completed_at DATETIME(6) NULL, version BIGINT NOT NULL DEFAULT 1,
  created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
  created_by VARCHAR(255) NOT NULL, updated_by VARCHAR(255) NOT NULL,
  UNIQUE KEY uq_workflow_task_node (instance_id,node_id),
  INDEX idx_workflow_tasks_inbox (tenant_id,status,assignee_type,assignee,created_at,id),
  INDEX idx_workflow_tasks_instance (tenant_id,instance_id,created_at,id),
  CONSTRAINT fk_workflow_task_instance FOREIGN KEY (instance_id) REFERENCES workflow_instances(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE workflow_task_history (
  id VARCHAR(36) PRIMARY KEY, tenant_id VARCHAR(255) NOT NULL, task_id VARCHAR(36) NOT NULL,
  instance_id VARCHAR(36) NOT NULL, action VARCHAR(64) NOT NULL, actor_id VARCHAR(255) NOT NULL,
  from_status VARCHAR(32) NOT NULL, to_status VARCHAR(32) NOT NULL, detail_json LONGTEXT NOT NULL,
  version BIGINT NOT NULL DEFAULT 1, created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
  created_by VARCHAR(255) NOT NULL, updated_by VARCHAR(255) NOT NULL,
  INDEX idx_workflow_task_history_task (tenant_id,task_id,created_at,id),
  INDEX idx_workflow_task_history_retention (created_at,id),
  CONSTRAINT fk_workflow_history_task FOREIGN KEY (task_id) REFERENCES workflow_tasks(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE workflow_outbox_events (
  id VARCHAR(36) PRIMARY KEY, subject VARCHAR(255) NOT NULL, envelope LONGBLOB NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, available_at DATETIME(6) NOT NULL, published_at DATETIME(6) NULL,
  last_error TEXT NOT NULL, version BIGINT NOT NULL DEFAULT 1, created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL, created_by VARCHAR(255) NOT NULL, updated_by VARCHAR(255) NOT NULL,
  INDEX idx_workflow_outbox_pending (published_at,available_at,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
