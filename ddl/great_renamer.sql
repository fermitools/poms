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
-- spw missed on 2nd
ALTER TABLE launch_template_snapshots RENAME COLUMN launch_snapshot_id TO login_setup_snapshot_id;
ALTER TABLE submissions RENAME COLUMN launch_snapshot_id TO login_setup_snapshot_id;


ALTER TABLE campaign_definitions RENAME COLUMN campaign_definition_id TO job_type_id;
ALTER TABLE campaigns            RENAME COLUMN campaign_definition_id TO job_type_id;
ALTER TABLE campaign_snapshots   RENAME COLUMN campaign_definition_id TO job_type_id;
ALTER TABLE campaign_recoveries  RENAME COLUMN campaign_definition_id TO job_type_id;
-- overlooked on first pass
ALTER TABLE campaign_definition_snapshots RENAME COLUMN campaign_definition_id TO job_type_id;


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
-- spw missed on 2nd
ALTER TABLE launch_template_snapshots     RENAME TO login_setup_snapshots;
ALTER TABLE campaign_definitions          RENAME TO job_types;
ALTER TABLE campaign_snapshots            RENAME TO campaign_stage_snapshots;
ALTER TABLE campaign_definition_snapshots RENAME TO job_type_snapshots;
ALTER TABLE campaigns                     RENAME TO campaign_stages;
ALTER TABLE tasks                         RENAME to submissions;
ALTER TABLE tags                          RENAME TO campaigns;
ALTER TABLE campaigns_tags                RENAME TO campaign_campaign_stages;

-- missed on first pass
DROP TRIGGER update_task_history on submissions;
DROP FUNCTION update_task_history();

ALTER TABLE task_histories                RENAME TO submission_histories;

CREATE OR REPLACE FUNCTION update_submission_history()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'INSERT' or NEW.status != OLD.status THEN
       INSERT INTO submission_histories SELECT NEW.submission_id, now(), NEW.status;
    END IF;
    RETURN NULL;
END;
$function$;

CREATE TRIGGER update_submission_history AFTER INSERT OR UPDATE ON submissions FOR EACH ROW EXECUTE PROCEDURE update_submission_history();


-- spw sequences
ALTER SEQUENCE launch_template_snapshots_launch_snapshot_id_seq RENAME TO login_setup_snapshots_login_setup_id_seq;
ALTER SEQUENCE campaign_definition_snapshots_campaign_definition_snap_id_seq RENAME TO job_type_snapshots_job_type_snapshot_id_seq;
ALTER SEQUENCE campaign_definitions_campaign_definition_id_seq RENAME TO job_types_job_type_id_seq;
ALTER SEQUENCE campaigns_campaign_id_seq RENAME TO campaign_stages_campaign_stage_id_seq;
ALTER SEQUENCE launch_templates_launch_id_seq RENAME TO login_setups_login_setup_id_seq;
ALTER SEQUENCE tasks_task_id_seq RENAME TO submissions_submissions_id_seq;
ALTER SEQUENCE tags_tag_id_seq RENAME TO campaigns_campaign_id_seq;
ALTER SEQUENCE campaign_snapshots_campaign_snapshot_id_seq RENAME TO campaign_stage_snapshots_campaign_stage_snapshot_id_seq;

-- Changes for moving jobs to fifemon
ALTER TABLE submissions ADD jobsub_job_id text   ;
ALTER TABLE submissions DROP COLUMN status;

DROP TABLE job_histories ;
DROP TABLE job_files ;
DROP TABLE jobs ;
DROP TABLE services_downtimes;
DROP TABLE services;
DROP FUNCTION update_job_history();
DROP FUNCTION update_task_history();


