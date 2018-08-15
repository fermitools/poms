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
ALTER TABLE tasks RENAME COLUMN launch_snapshot_id TO login_setup_snapshot_id;


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
-- ALTER TABLE campaigns_tags      RENAME COLUMN campaign_id to campaign_stage_id;
ALTER TABLE campaign_snapshots  RENAME COLUMN campaign_id to campaign_stage_id;
ALTER TABLE held_launches       RENAME COLUMN campaign_id to campaign_stage_id;
ALTER TABLE campaign_dependencies RENAME COLUMN needs_camp_id to needs_campaign_stage_id;
ALTER TABLE campaign_dependencies RENAME COLUMN uses_camp_id to provides_campaign_stage_id;

-- ALTER TABLE tags           RENAME COLUMN tag_id to campaign_id;
-- ALTER TABLE campaigns_tags RENAME COLUMN tag_id to campaign_id;

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
-- ALTER TABLE tags                          RENAME TO campaigns;
-- ALTER TABLE campaigns_tags                RENAME TO campaign_campaign_stages;

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
-- ALTER SEQUENCE tags_tag_id_seq RENAME TO campaigns_campaign_id_seq;
ALTER SEQUENCE campaign_snapshots_campaign_snapshot_id_seq RENAME TO campaign_stage_snapshots_campaign_stage_snapshot_id_seq;

-- Changes for moving jobs to fifemon
ALTER TABLE submissions ADD jobsub_job_id text   ;
ALTER TABLE submissions DROP COLUMN status;

DROP TABLE job_histories ;
DROP TABLE job_files ;
DROP TABLE jobs ;
DROP TABLE service_downtimes;
DROP TABLE services;
DROP FUNCTION update_job_history();
drop trigger update_submission_history on  submissions;
drop function update_submission_history();


-- Changes for adding a campaigns table and moving tags to it.
ALTER TABLE campaigns_tags drop constraint fk_campaigns_tags_0;
alter table campaign_stages add column campaign_id integer;
CREATE INDEX idx_campaign_stages_campaign_id ON campaign_stages ( campaign_id ) ;

CREATE TABLE campaigns (
	campaign_id          serial  NOT NULL ,
	experiment           varchar(10)  NOT NULL ,
	name                 text  ,
	creator              integer  NOT NULL ,
	creator_role         text  NOT NULL ,
	defaults             json   ,
	updater              integer   ,
	updated              timestamptz   ,
	CONSTRAINT pk_campaigns_campaign_id PRIMARY KEY ( campaign_id )
 );

CREATE INDEX idx_campaigns_experiment ON campaigns ( experiment );

COMMENT ON COLUMN campaigns.experiment IS 'Acroynm for the experiment';

ALTER TABLE campaigns ADD CONSTRAINT fk_campaigns_experiments FOREIGN KEY ( experiment ) REFERENCES experiments( experiment );

ALTER TABLE campaign_stages ADD CONSTRAINT fk_campaign_stages_campaigns FOREIGN KEY ( campaign_id) REFERENCES campaigns( campaign_id );

grant select, insert, update, delete on all tables in schema public to pomsdbs;
grant usage on all sequences in schema public to pomsdbs;

CREATE UNIQUE INDEX ON campaigns (experiment, name) WHERE name IS NOT NULL;

-- temp table keep for a while in case of conversion issues.
create table campaigns_tags_old as (select * from campaigns_tags);
grant all on campaigns_tags_old to pomsdbs;
-- this will be reloaded, so kill'em
delete from campaigns_tags;

ALTER TABLE campaigns_tags ADD CONSTRAINT fk_campaigns_tags_campaigns FOREIGN KEY ( campaign_id ) REFERENCES campaigns( campaign_id );
-- do I need this or is it already there?  let's be sure
ALTER TABLE campaigns_tags ADD CONSTRAINT fk_campaigns_tags FOREIGN KEY ( tag_id ) REFERENCES tags ( tag_id );
