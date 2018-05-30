
 ALTER TABLE launch_templates RENAME TO login_setups;
 ALTER TABLE login_setups RENAME COLUMN launch_template_id TO login_setup_id;
 ALTER TABLE campaign_definitions RENAME TO job_types;
 ALTER TABLE job_types RENAME COLUMN campaign_definition_id TO job_type_id;
 ALTER TABLE campaign_snapshots RENAME TO campaign_stage_snapshots;
 ALTER TABLE campaign_stage_snapshots RENAME COLUMN campaign_snapshot_id TO campaign_stage_snapshot_id;
 ALTER TABLE campaign_definition_snapshots RENAME TO job_type_snapshots;
 ALTER TABLE job_type_snapshots RENAME COLUMN campaign_definition_snap_id  TO job_type_snapshot_id;
 ALTER TABLE campaigns RENAME TO campaign_stages;
 ALTER TABLE campaign_stages RENAME COLUMN campaign_id to campaign_stage_id;
 ALTER TABLE tasks RENAME to submissions;
 ALTER TABLE submissions RENAME COLUMN task_id TO submission_id;
 ALTER TABLE submissions RENAME COLUMN task_params TO submission_params;
 ALTER TABLE tags RENAME TO campaigns;
 ALTER TABLE campaigns RENAME COLUMN tag_id to campaign_id;
 ALTER TABLE campaigns_tags RENAME TO campaigns_campaign_stages;
