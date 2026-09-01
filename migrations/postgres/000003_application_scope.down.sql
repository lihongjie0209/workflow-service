DROP INDEX idx_workflow_task_history_task;
CREATE INDEX idx_workflow_task_history_task ON workflow_task_history(tenant_id,task_id,created_at,id);
DROP INDEX idx_workflow_tasks_instance;
CREATE INDEX idx_workflow_tasks_instance ON workflow_tasks(tenant_id,instance_id,created_at,id);
DROP INDEX idx_workflow_tasks_inbox;
CREATE INDEX idx_workflow_tasks_inbox ON workflow_tasks(tenant_id,status,assignee_type,assignee,created_at DESC,id);
DROP INDEX idx_workflow_instances_definition;
CREATE INDEX idx_workflow_instances_definition ON workflow_instances(tenant_id,definition_id,started_at DESC,id);
DROP INDEX idx_workflow_instances_list;
CREATE INDEX idx_workflow_instances_list ON workflow_instances(tenant_id,status,started_at DESC,id);
DROP INDEX idx_workflow_revisions_tenant;
CREATE INDEX idx_workflow_revisions_tenant ON workflow_definition_revisions(tenant_id,definition_key,revision DESC);

ALTER TABLE workflow_instances DROP CONSTRAINT uq_workflow_instance_application_idempotency;
ALTER TABLE workflow_instances ADD CONSTRAINT workflow_instances_tenant_id_idempotency_key_key UNIQUE (tenant_id,idempotency_key);
ALTER TABLE workflow_definitions DROP CONSTRAINT uq_workflow_definition_application_key;
ALTER TABLE workflow_definitions ADD CONSTRAINT workflow_definitions_tenant_id_definition_key_key UNIQUE (tenant_id,definition_key);

ALTER TABLE workflow_task_history DROP COLUMN application_id;
ALTER TABLE workflow_tasks DROP COLUMN application_id;
ALTER TABLE workflow_instances DROP COLUMN application_id;
