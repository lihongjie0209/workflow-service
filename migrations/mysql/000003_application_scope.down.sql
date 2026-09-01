ALTER TABLE workflow_task_history
  DROP INDEX idx_workflow_task_history_task,
  ADD INDEX idx_workflow_task_history_task (tenant_id,task_id,created_at,id);
ALTER TABLE workflow_tasks
  DROP INDEX idx_workflow_tasks_instance,
  ADD INDEX idx_workflow_tasks_instance (tenant_id,instance_id,created_at,id),
  DROP INDEX idx_workflow_tasks_inbox,
  ADD INDEX idx_workflow_tasks_inbox (tenant_id,status,assignee_type,assignee,created_at,id);
ALTER TABLE workflow_instances
  DROP INDEX idx_workflow_instances_definition,
  ADD INDEX idx_workflow_instances_definition (tenant_id,definition_id,started_at,id),
  DROP INDEX idx_workflow_instances_list,
  ADD INDEX idx_workflow_instances_list (tenant_id,status,started_at,id);
ALTER TABLE workflow_definition_revisions
  DROP INDEX idx_workflow_revisions_tenant,
  ADD INDEX idx_workflow_revisions_tenant (tenant_id,definition_key,revision);

ALTER TABLE workflow_instances
  DROP INDEX uq_workflow_instance_application_idempotency,
  ADD UNIQUE KEY uq_workflow_instance_idempotency (tenant_id,idempotency_key);
ALTER TABLE workflow_definitions
  DROP INDEX uq_workflow_definition_application_key,
  ADD UNIQUE KEY uq_workflow_definition_key (tenant_id,definition_key);

ALTER TABLE workflow_task_history DROP COLUMN application_id;
ALTER TABLE workflow_tasks DROP COLUMN application_id;
ALTER TABLE workflow_instances DROP COLUMN application_id;
