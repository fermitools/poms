do $$
declare
  rolename varchar;
  begin
    select current_database() into rolename;
    execute 'set session role ' || rolename;
  end;
$$ language 'plpgsql';
\set ECHO all
show role;
set client_min_messages to warning;

-- begin
ALTER TABLE launch_templates          RENAME COLUMN launch_id TO login_setup_id;
ALTER TABLE campaigns                 RENAME COLUMN launch_id TO login_setup_id;
ALTER TABLE campaign_snapshots        RENAME COLUMN launch_id TO login_setup_id;
ALTER TABLE launch_template_snapshots RENAME COLUMN launch_id TO login_setup_id;


ALTER TABLE campaign_definitions RENAME COLUMN campaign_definition_id TO job_type_id;
ALTER TABLE campaigns            RENAME COLUMN campaign_definition_id TO job_type_id;
ALTER TABLE campaign_snapshots   RENAME COLUMN campaign_definition_id TO job_type_id;
ALTER TABLE campaign_recoveries  RENAME COLUMN campaign_definition_id TO job_type_id;


ALTER TABLE campaign_snapshots RENAME COLUMN campaign_snapshot_id TO campaign_stage_snapshot_id;
ALTER TABLE tasks              RENAME COLUMN campaign_snapshot_id TO campaign_stage_snapshot_id;

ALTER TABLE campaign_definition_snapshots RENAME COLUMN campaign_definition_snap_id  TO job_type_snapshot_id;
ALTER TABLE tasks                         RENAME COLUMN campaign_definition_snap_id  TO job_type_snapshot_id;

ALTER TABLE campaigns           RENAME COLUMN campaign_id to campaign_stage_id;
ALTER TABLE tasks               RENAME COLUMN campaign_id to campaign_stage_id;
ALTER TABLE campaigns_tags      RENAME COLUMN campaign_id to campaign_stage_id;
ALTER TABLE campaign_snapshots  RENAME COLUMN campaign_id to campaign_stage_id;
ALTER TABLE held_launches       RENAME COLUMN campaign_id to campaign_stage_id;
ALTER TABLE campaign_dependencies RENAME COLUMN needs_camp_id to needs_campaign_stage_id;
ALTER TABLE campaign_dependencies RENAME COLUMN uses_camp_id to provides_campaign_stage_id;

ALTER TABLE tags           RENAME COLUMN tag_id to campaign_id;
ALTER TABLE campaigns_tags RENAME COLUMN tag_id to campaign_id;

ALTER TABLE tasks                   RENAME COLUMN task_id TO submission_id;
ALTER TABLE jobs                    RENAME COLUMN task_id TO submission_id;
ALTER TABLE task_histories          RENAME COLUMN task_id TO submission_id;

ALTER TABLE held_launches           RENAME column parent_task_id TO parent_submission_id;
ALTER TABLE tasks                   RENAME COLUMN task_parameters TO submission_params;

ALTER TABLE launch_templates              RENAME TO login_setups;
ALTER TABLE campaign_definitions          RENAME TO job_types;
ALTER TABLE campaign_snapshots            RENAME TO campaign_stage_snapshots;
ALTER TABLE campaign_definition_snapshots RENAME TO job_type_snapshots;
ALTER TABLE campaigns                     RENAME TO campaign_stages;
ALTER TABLE tasks                         RENAME to submissions;
ALTER TABLE tags                          RENAME TO campaigns;
ALTER TABLE campaigns_tags                RENAME TO campaign_campaign_stages;
