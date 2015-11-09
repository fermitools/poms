--CREATE SCHEMA "public";

CREATE TABLE experimenters ( 
	experimenter_id      serial  NOT NULL,
	first_name           text  NOT NULL,
	last_name            text  ,
	email                text  NOT NULL,
	CONSTRAINT pk_expermenter PRIMARY KEY ( experimenter_id )
 );

CREATE TABLE experiments ( 
	experiment           varchar(10)  NOT NULL,
	name                 text  NOT NULL,
	CONSTRAINT pk_experiments PRIMARY KEY ( experiment )
 );

COMMENT ON COLUMN experiments.experiment IS 'Acroynm for the experiment';

CREATE TABLE experiments_experimenters ( 
	experiment           text  NOT NULL,
	experimenter_id      integer  NOT NULL,
	active               bool DEFAULT true NOT NULL,
	CONSTRAINT pk_experiment_expermenters PRIMARY KEY ( experimenter_id, experiment )
 );

CREATE INDEX idx_experiment_expermenters ON experiments_experimenters ( experiment );

CREATE TABLE task_definitions ( 
	task_definition_id   serial  NOT NULL,
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
	CONSTRAINT pk_task_definitions PRIMARY KEY ( task_definition_id ),
	CONSTRAINT idx_task_definitions_name UNIQUE ( name ) 
 );

CREATE INDEX idx_task_definitions_experiment ON task_definitions ( experiment );

CREATE INDEX idx_task_definitions_creator ON task_definitions ( creator );

CREATE INDEX idx_task_definitions_updater ON task_definitions ( updater );

COMMENT ON COLUMN task_definitions.experiment IS 'Acroynm for the experiment';

CREATE TABLE campaigns ( 
	campaign_id          serial  NOT NULL,
	experiment           text  NOT NULL,
	name                 text  NOT NULL,
	task_definition_id   serial  NOT NULL,
	vo_role              text NOT NULL,
	creator              integer  NOT NULL,
	created              timestamptz  NOT NULL,
	updater              integer  ,
	updated              timestamptz  ,
	CONSTRAINT pk_campaigns PRIMARY KEY ( campaign_id ),
	CONSTRAINT idx_campaigns_experiment_name UNIQUE ( experiment, name ) 
 );

CREATE INDEX idx_campaigns ON campaigns ( experiment );

CREATE INDEX idx_campaigns_creator ON campaigns ( creator );

CREATE INDEX idx_campaigns_task_definition_id ON campaigns ( task_definition_id );

CREATE INDEX idx_campaigns_0 ON campaigns ( updater );

CREATE TABLE tasks ( 
	task_id              serial  NOT NULL,
	campaign_id          serial  NOT NULL,
	task_definition_id   serial  NOT NULL,
	task_order           integer  NOT NULL,
	input_dataset        text  NOT NULL,
	output_dataset       text  NOT NULL,
	creator              integer  NOT NULL,
	created              timestamptz  NOT NULL,
	status               text  NOT NULL,
	task_parameters      json  ,
	depends_on           integer  ,
	depend_threshold     integer  ,
	updater              integer  ,
	updated              timestamptz  ,
	command_executed     text  ,
	project              text  ,
	CONSTRAINT pk_tasks PRIMARY KEY ( task_id )
 );

CREATE INDEX idx_tasks ON tasks ( campaign_id );

CREATE INDEX idx_tasks_depends_on ON tasks ( depends_on );

CREATE INDEX idx_tasks_task_definition_id ON tasks ( task_definition_id );

CREATE INDEX idx_tasks_creator ON tasks ( creator );

CREATE INDEX idx_tasks_updater ON tasks ( updater );

COMMENT ON COLUMN tasks.command_executed IS 'The actual command executed to produce the jobs.';

CREATE TABLE jobs ( 
	job_id               bigserial  NOT NULL,
	task_id              integer  NOT NULL,
	jobsub_job_id        text  NOT NULL,
	node_name            text  NOT NULL,
	cpu_type             text  NOT NULL,
	host_site            text  NOT NULL,
	status               text  NOT NULL,
	updated              timestamptz  NOT NULL,
	output_files_declared bool DEFAULT false NOT NULL,
	output_file_names    text  ,
	user_exe_exit_code   integer  ,
	CONSTRAINT pk_jobs PRIMARY KEY ( job_id )
 );

CREATE INDEX idx_jobs_task_id ON jobs ( task_id );

CREATE TABLE task_histories ( 
	task_id              integer  NOT NULL,
	created              timestamptz  NOT NULL,
	status               text  NOT NULL,
	CONSTRAINT pk_task_histories PRIMARY KEY ( task_id, created )
 );

CREATE TABLE job_histories ( 
	job_id               bigint  NOT NULL,
	created              timestamptz  NOT NULL,
	status               text  NOT NULL,
	CONSTRAINT pk_job_histories PRIMARY KEY ( job_id, created )
 );

CREATE TABLE service_downtimes ( 
	service_id           integer  NOT NULL,
	downtime_type        text  NOT NULL,
	downtime_started     timestamptz  NOT NULL,
	downtime_ended       timestamptz  ,
	CONSTRAINT pk_service_downtimes PRIMARY KEY ( service_id, downtime_started )
 );

ALTER TABLE service_downtimes ADD CONSTRAINT ck_downtime_type CHECK ( downtime_type::text = ANY (ARRAY['actual'::character varying, 'scheduled'::character varying]::text[]) );

CREATE TABLE services ( 
	service_id           serial  NOT NULL,
	name                 text  NOT NULL,
	host_site            text  NOT NULL,
	status               text  NOT NULL,
	updated              timestamptz  NOT NULL,
	active               bool DEFAULT true NOT NULL,
	description          text  ,
	parent_service_id    integer  ,
	url                  text  ,
	items                integer  ,
	failed_items         integer  ,
	CONSTRAINT pk_services PRIMARY KEY ( service_id )
 );

CREATE INDEX idx_services_parent_service_id ON services ( parent_service_id );

CREATE OR REPLACE FUNCTION experiments_lowercase_experiment()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    BEGIN        
        NEW.experiment = LOWER(NEW.experiment);
        RETURN NEW;
    END;
$function$;

CREATE OR REPLACE FUNCTION update_job_history()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
   
    IF TG_OP = 'INSERT' or  NEW.status != OLD.status THEN
        INSERT INTO job_histories SELECT NEW.job_id, now(), NEW.status;
    END IF;
    RETURN NULL;
END;
$function$;

CREATE OR REPLACE FUNCTION update_task_history()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'INSERT' or NEW.status != OLD.status THEN
       INSERT INTO task_histories SELECT NEW.task_id, now(), NEW.status;
    END IF;
    RETURN NULL;
END;
$function$;

CREATE TRIGGER experiments_lowercase_experiment BEFORE INSERT OR UPDATE ON experiments FOR EACH ROW EXECUTE PROCEDURE experiments_lowercase_experiment();

CREATE TRIGGER update_job_history AFTER INSERT OR UPDATE ON jobs FOR EACH ROW EXECUTE PROCEDURE update_job_history();

CREATE TRIGGER update_task_history AFTER INSERT OR UPDATE ON tasks FOR EACH ROW EXECUTE PROCEDURE update_task_history();

ALTER TABLE campaigns ADD CONSTRAINT fk_campaigns FOREIGN KEY ( experiment ) REFERENCES experiments( experiment );

ALTER TABLE campaigns ADD CONSTRAINT fk_campaigns_definitions FOREIGN KEY ( task_definition_id ) REFERENCES task_definitions( task_definition_id );

ALTER TABLE campaigns ADD CONSTRAINT fk_campaigns_updater FOREIGN KEY ( updater ) REFERENCES experimenters( experimenter_id );

ALTER TABLE campaigns ADD CONSTRAINT fk_campaigns_creator FOREIGN KEY ( creator ) REFERENCES experimenters( experimenter_id );

ALTER TABLE experiments_experimenters ADD CONSTRAINT fk_experiment_expermenters FOREIGN KEY ( experiment ) REFERENCES experiments( experiment );

ALTER TABLE experiments_experimenters ADD CONSTRAINT fk_experiment_expermenters_0 FOREIGN KEY ( experimenter_id ) REFERENCES experimenters( experimenter_id );

ALTER TABLE job_histories ADD CONSTRAINT fk_job_histories FOREIGN KEY ( job_id ) REFERENCES jobs( job_id );

ALTER TABLE jobs ADD CONSTRAINT fk_jobs FOREIGN KEY ( task_id ) REFERENCES tasks( task_id );

ALTER TABLE service_downtimes ADD CONSTRAINT fk_service_downtimes FOREIGN KEY ( service_id ) REFERENCES services( service_id );

ALTER TABLE services ADD CONSTRAINT fk_services FOREIGN KEY ( parent_service_id ) REFERENCES services( service_id );

ALTER TABLE task_definitions ADD CONSTRAINT fk_task_definitions FOREIGN KEY ( experiment ) REFERENCES experiments( experiment );

ALTER TABLE task_definitions ADD CONSTRAINT fk_task_definitions_creator FOREIGN KEY ( creator ) REFERENCES experimenters( experimenter_id );

ALTER TABLE task_definitions ADD CONSTRAINT fk_task_definitions_updater FOREIGN KEY ( updater ) REFERENCES experimenters( experimenter_id );

ALTER TABLE task_histories ADD CONSTRAINT fk_task_histories FOREIGN KEY ( task_id ) REFERENCES tasks( task_id );

ALTER TABLE tasks ADD CONSTRAINT fk_tasks FOREIGN KEY ( campaign_id ) REFERENCES campaigns( campaign_id );

ALTER TABLE tasks ADD CONSTRAINT fk_tasks_creator FOREIGN KEY ( creator ) REFERENCES experimenters( experimenter_id );

ALTER TABLE tasks ADD CONSTRAINT fk_tasks_updater FOREIGN KEY ( updater ) REFERENCES experimenters( experimenter_id );

