set role pomsprd;

/* Adding the job_files table and moving the column from jobs to this table */

CREATE TABLE job_files ( 
	job_id               bigint  NOT NULL,
	file_name            text  NOT NULL,
	file_type            text  NOT NULL,
	created              timestamptz  NOT NULL,
	declared             timestamptz  ,
	CONSTRAINT pk_output_files PRIMARY KEY ( job_id, file_name )
 ) ;

ALTER TABLE job_files ADD CONSTRAINT check_file_type CHECK ( (file_type = 'input'::text) OR (file_type = 'output'::text) OR (file_type = 'log'::text) ) ;

ALTER TABLE job_files ADD CONSTRAINT fk_job_files FOREIGN KEY ( job_id ) REFERENCES jobs( job_id )    ;



/* Adding Snapshot tables */

ALTER TABLE tasks ADD launch_snapshot_id integer;

ALTER TABLE tasks ADD campaign_snapshot_id integer;

ALTER TABLE tasks ADD campaign_definition_snap_id integer;

CREATE INDEX idx_tasks_launch_snapshot_id ON tasks ( launch_snapshot_id ) ;

CREATE INDEX idx_tasks_campaign_snapshot_id ON tasks ( campaign_snapshot_id ) ;

CREATE INDEX idx_tasks_campaign_definition_snap_id ON tasks ( campaign_definition_snap_id ) ;

CREATE TABLE campaign_definition_snapshots ( 
	campaign_definition_snap_id serial  NOT NULL,
	campaign_definition_id integer  NOT NULL,
	name                 text  NOT NULL,
	experiment           text  NOT NULL,
	launch_script        text  ,
	definition_parameters json  ,
	input_files_per_job  integer  ,
	output_files_per_job integer  ,
	creator              integer  NOT NULL,
	created              timestamptz  NOT NULL,
	updater              integer  ,
	updated              timestamptz  ,
	CONSTRAINT pk_campaign_definition_snapshots PRIMARY KEY ( campaign_definition_snap_id )
 ) ;

CREATE INDEX idx_campaign_definition_snapshots_name ON campaign_definition_snapshots ( campaign_definition_id ) ;

COMMENT ON COLUMN campaign_definition_snapshots.experiment IS 'Acroynm for the experiment';

CREATE TABLE campaign_snapshots ( 
	campaign_snapshot_id serial  NOT NULL,
	campaign_id          integer  NOT NULL,
	experiment           text  NOT NULL,
	name                 text  NOT NULL,
	campaign_definition_id integer  NOT NULL,
	vo_role              text  NOT NULL,
	creator              integer  NOT NULL,
	created              timestamptz  NOT NULL,
	active               bool DEFAULT true NOT NULL,
	dataset              text  NOT NULL,
	software_version     text  NOT NULL,
	launch_id            integer  NOT NULL,
	param_overrides      json  ,
	updater              integer  ,
	updated              timestamptz  ,
	cs_last_split        timestamptz  ,
	cs_split_type        text  ,
	cs_split_dimensions  text  ,
	CONSTRAINT idx_campaign_snapshots PRIMARY KEY ( campaign_snapshot_id )
 ) ;

CREATE INDEX idx_campaign_snapshots_campaign_id ON campaign_snapshots ( campaign_id ) ;

CREATE TABLE launch_template_snapshots ( 
	launch_snapshot_id   serial  NOT NULL,
	launch_id            integer  NOT NULL,
	experiment           varchar(10)  NOT NULL,
	launch_host          text  NOT NULL,
	launch_account       text  NOT NULL,
	launch_setup         text  NOT NULL,
	creator              integer  NOT NULL,
	created              timestamptz  NOT NULL,
	updater              integer  ,
	updated              timestamptz  ,
	name                 text  NOT NULL,
	CONSTRAINT pk_launch_template_snapshots PRIMARY KEY ( launch_snapshot_id )
 ) ;

CREATE INDEX idx_launch_template_snapshots_launch_id ON launch_template_snapshots ( launch_id ) ;

COMMENT ON COLUMN launch_template_snapshots.experiment IS 'Acroynm for the experiment';

ALTER TABLE campaign_definition_snapshots ADD CONSTRAINT fk_campaign_definition_snapshots FOREIGN KEY ( campaign_definition_id ) REFERENCES campaign_definitions( campaign_definition_id )    ;

ALTER TABLE campaign_snapshots ADD CONSTRAINT fk_campaign_snapshots FOREIGN KEY ( campaign_id ) REFERENCES campaigns( campaign_id )    ;

ALTER TABLE launch_template_snapshots ADD CONSTRAINT fk_launch_template_snapshots FOREIGN KEY ( launch_id ) REFERENCES launch_templates( launch_id )    ;



ALTER TABLE tasks ADD CONSTRAINT fk_tasks_launch_snapshot_id FOREIGN KEY ( launch_snapshot_id ) REFERENCES launch_template_snapshots( launch_snapshot_id )    ;

ALTER TABLE tasks ADD CONSTRAINT fk_tasks_campaign_snapshot_id FOREIGN KEY ( campaign_snapshot_id ) REFERENCES campaign_snapshots( campaign_snapshot_id )    ;

ALTER TABLE tasks ADD CONSTRAINT fk_tasks_campaign_definition_snap_id FOREIGN KEY ( campaign_definition_snap_id ) REFERENCES campaign_definition_snapshots( campaign_definition_snap_id )    ;

ALTER TABLE campaign_definitions ADD output_file_patterns text  ;



/* Adding recovery types */

ALTER TABLE tasks ADD recovery_position integer  ;

CREATE TABLE recovery_types ( 
	recovery_type_id     serial  NOT NULL,
	name                 text  NOT NULL,
	description          text  NOT NULL,
	CONSTRAINT pk_recovery_type PRIMARY KEY ( recovery_type_id )
 ) ;

CREATE TABLE campaign_recoveries ( 
	campaign_definition_id integer  NOT NULL,
	recovery_type_id     integer  NOT NULL,
	recovery_order       integer  NOT NULL,
	CONSTRAINT pk_campaign_recoveries PRIMARY KEY ( campaign_definition_id, recovery_type_id )
 ) ;

CREATE INDEX idx_campaign_recoveries ON campaign_recoveries ( recovery_type_id ) ;

ALTER TABLE campaign_recoveries ADD CONSTRAINT fk_campaign_recoveries FOREIGN KEY ( recovery_type_id ) REFERENCES recovery_types( recovery_type_id )    ;

ALTER TABLE campaign_recoveries ADD CONSTRAINT fk_campaign_recoveries_0 FOREIGN KEY ( campaign_definition_id ) REFERENCES campaign_definitions( campaign_definition_id )    ;

/* Clean up discrpencies */

ALTER TABLE jobs DROP COLUMN output_file_names;


grant select, insert, update, delete on all tables in schema public to pomsdbs;
grant usage on all sequences in schema public to pomsdbs;
