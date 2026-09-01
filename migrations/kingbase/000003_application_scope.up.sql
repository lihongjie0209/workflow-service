ALTER TABLE workflow_instances ADD COLUMN application_id TEXT;
UPDATE workflow_instances i
SET application_id = d.application_id
FROM workflow_definitions d
WHERE d.id = i.definition_id;
ALTER TABLE workflow_instances ALTER COLUMN application_id SET NOT NULL;

ALTER TABLE workflow_tasks ADD COLUMN application_id TEXT;
UPDATE workflow_tasks t
SET application_id = i.application_id
FROM workflow_instances i
WHERE i.id = t.instance_id;
ALTER TABLE workflow_tasks ALTER COLUMN application_id SET NOT NULL;

ALTER TABLE workflow_task_history ADD COLUMN application_id TEXT;
UPDATE workflow_task_history h
SET application_id = t.application_id
FROM workflow_tasks t
WHERE t.id = h.task_id;
ALTER TABLE workflow_task_history ALTER COLUMN application_id SET NOT NULL;

ALTER TABLE workflow_definitions DROP CONSTRAINT workflow_definitions_tenant_id_definition_key_key;
ALTER TABLE workflow_definitions ADD CONSTRAINT uq_workflow_definition_application_key UNIQUE (tenant_id,application_id,definition_key);

ALTER TABLE workflow_instances DROP CONSTRAINT workflow_instances_tenant_id_idempotency_key_key;
ALTER TABLE workflow_instances ADD CONSTRAINT uq_workflow_instance_application_idempotency UNIQUE (tenant_id,application_id,idempotency_key);

DROP INDEX idx_workflow_revisions_tenant;
CREATE INDEX idx_workflow_revisions_tenant ON workflow_definition_revisions(tenant_id,application_id,definition_key,revision DESC);
DROP INDEX idx_workflow_instances_list;
CREATE INDEX idx_workflow_instances_list ON workflow_instances(tenant_id,application_id,status,started_at DESC,id);
DROP INDEX idx_workflow_instances_definition;
CREATE INDEX idx_workflow_instances_definition ON workflow_instances(tenant_id,application_id,definition_id,started_at DESC,id);
DROP INDEX idx_workflow_tasks_inbox;
CREATE INDEX idx_workflow_tasks_inbox ON workflow_tasks(tenant_id,application_id,status,assignee_type,assignee,created_at DESC,id);
DROP INDEX idx_workflow_tasks_instance;
CREATE INDEX idx_workflow_tasks_instance ON workflow_tasks(tenant_id,application_id,instance_id,created_at,id);
DROP INDEX idx_workflow_task_history_task;
CREATE INDEX idx_workflow_task_history_task ON workflow_task_history(tenant_id,application_id,task_id,created_at,id);
