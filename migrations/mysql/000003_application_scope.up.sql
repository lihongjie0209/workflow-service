ALTER TABLE workflow_instances ADD COLUMN application_id VARCHAR(36) NULL AFTER tenant_id;
UPDATE workflow_instances i
JOIN workflow_definitions d ON d.id = i.definition_id
SET i.application_id = d.application_id;
ALTER TABLE workflow_instances MODIFY COLUMN application_id VARCHAR(36) NOT NULL;

ALTER TABLE workflow_tasks ADD COLUMN application_id VARCHAR(36) NULL AFTER tenant_id;
UPDATE workflow_tasks t
JOIN workflow_instances i ON i.id = t.instance_id
SET t.application_id = i.application_id;
ALTER TABLE workflow_tasks MODIFY COLUMN application_id VARCHAR(36) NOT NULL;

ALTER TABLE workflow_task_history ADD COLUMN application_id VARCHAR(36) NULL AFTER tenant_id;
UPDATE workflow_task_history h
JOIN workflow_tasks t ON t.id = h.task_id
SET h.application_id = t.application_id;
ALTER TABLE workflow_task_history MODIFY COLUMN application_id VARCHAR(36) NOT NULL;

ALTER TABLE workflow_definitions
  DROP INDEX uq_workflow_definition_key,
  ADD UNIQUE KEY uq_workflow_definition_application_key (tenant_id,application_id,definition_key);
ALTER TABLE workflow_instances
  DROP INDEX uq_workflow_instance_idempotency,
  ADD UNIQUE KEY uq_workflow_instance_application_idempotency (tenant_id,application_id,idempotency_key);

ALTER TABLE workflow_definition_revisions
  DROP INDEX idx_workflow_revisions_tenant,
  ADD INDEX idx_workflow_revisions_tenant (tenant_id,application_id,definition_key,revision);
ALTER TABLE workflow_instances
  DROP INDEX idx_workflow_instances_list,
  ADD INDEX idx_workflow_instances_list (tenant_id,application_id,status,started_at,id),
  DROP INDEX idx_workflow_instances_definition,
  ADD INDEX idx_workflow_instances_definition (tenant_id,application_id,definition_id,started_at,id);
ALTER TABLE workflow_tasks
  DROP INDEX idx_workflow_tasks_inbox,
  ADD INDEX idx_workflow_tasks_inbox (tenant_id,application_id,status,assignee_type,assignee,created_at,id),
  DROP INDEX idx_workflow_tasks_instance,
  ADD INDEX idx_workflow_tasks_instance (tenant_id,application_id,instance_id,created_at,id);
ALTER TABLE workflow_task_history
  DROP INDEX idx_workflow_task_history_task,
  ADD INDEX idx_workflow_task_history_task (tenant_id,application_id,task_id,created_at,id);
