--CREATE SCHEMA "public";

CREATE SEQUENCE campaigns_campaign_id_seq START WITH 1;

CREATE SEQUENCE campaigns_task_definition_id_seq START WITH 1;

CREATE SEQUENCE experimenters_experimenter_id_seq START WITH 1;

CREATE SEQUENCE "jobs_job_id _seq" START WITH 1;

CREATE SEQUENCE services_service_id_seq START WITH 1;

CREATE SEQUENCE task_definitions_task_definition_id_seq START WITH 1;

CREATE SEQUENCE tasks_campaign_id_seq START WITH 1;

CREATE SEQUENCE tasks_task_definition_id_seq START WITH 1;

CREATE SEQUENCE tasks_task_id_seq START WITH 1;

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
	task_definition_id   serial  NOT NULL,
	creator              integer  NOT NULL,
	created              timestamptz  NOT NULL,
	updater              integer  ,
	updated              timestamptz  ,
	CONSTRAINT pk_campaigns PRIMARY KEY ( campaign_id )
 );

CREATE INDEX idx_campaigns ON campaigns ( experiment );

CREATE INDEX idx_campaigns_creator ON campaigns ( creator );

CREATE INDEX idx_campaigns_task_definition_id ON campaigns ( task_definition_id );

CREATE TABLE jobs ( 
	"job_id "            bigserial  NOT NULL,
	task_id              integer  NOT NULL,
	node_name            text  NOT NULL,
	cpu_type             text  NOT NULL,
	host_site            text  NOT NULL,
	status               text  NOT NULL,
	updated              timestamptz  NOT NULL,
	CONSTRAINT pk_jobs PRIMARY KEY ( "job_id " )
 );

CREATE INDEX idx_jobs_task_id ON jobs ( task_id );

CREATE TABLE service_downtimes ( 
	service_id           integer  NOT NULL,
	downtime_started     timestamptz  NOT NULL,
	downtime_ended       timestamptz  ,
	CONSTRAINT pk_service_downtimes PRIMARY KEY ( service_id, downtime_started )
 );

CREATE TABLE services ( 
	service_id           serial  NOT NULL,
	name                 text  NOT NULL,
	host_site            text  NOT NULL,
	status               text  NOT NULL,
	updated              timestamptz  NOT NULL,
	active               bool DEFAULT true NOT NULL,
	parent_service_id    integer  ,
	url                  text  ,
	CONSTRAINT pk_services PRIMARY KEY ( service_id )
 );

CREATE INDEX idx_services_parent_service_id ON services ( parent_service_id );

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
	waiting_for          integer  ,
	waiting_threshold    integer  ,
	updater              integer  ,
	updated              timestamptz  ,
	CONSTRAINT pk_tasks PRIMARY KEY ( task_id )
 );

CREATE INDEX idx_tasks ON tasks ( campaign_id );

CREATE INDEX idx_tasks_waiting_for ON tasks ( waiting_for );

CREATE INDEX idx_tasks_task_definition_id ON tasks ( task_definition_id );

CREATE INDEX idx_tasks_creator ON tasks ( creator );

CREATE INDEX idx_tasks_updater ON tasks ( updater );

create trigger experiments_insert_update before insert or update on experiments
  for each row execute procedure experiments_lowercase_experiment();;

CREATE OR REPLACE FUNCTION public.experiments_lowercase_experiment()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    BEGIN        
        NEW.experiment = LOWER(NEW.experiment);
        RETURN NEW;
    END;
$function$

ALTER TABLE campaigns ADD CONSTRAINT fk_campaigns FOREIGN KEY ( experiment ) REFERENCES experiments( experiment );

ALTER TABLE campaigns ADD CONSTRAINT fk_campaigns_0 FOREIGN KEY ( creator ) REFERENCES experimenters( experimenter_id );

ALTER TABLE campaigns ADD CONSTRAINT fk_campaigns_definitions FOREIGN KEY ( task_definition_id ) REFERENCES task_definitions( task_definition_id );

ALTER TABLE experiments_experimenters ADD CONSTRAINT fk_experiment_expermenters FOREIGN KEY ( experiment ) REFERENCES experiments( experiment );

ALTER TABLE experiments_experimenters ADD CONSTRAINT fk_experiment_expermenters_0 FOREIGN KEY ( experimenter_id ) REFERENCES experimenters( experimenter_id );

ALTER TABLE jobs ADD CONSTRAINT fk_jobs FOREIGN KEY ( task_id ) REFERENCES tasks( task_id );

ALTER TABLE service_downtimes ADD CONSTRAINT fk_service_downtimes FOREIGN KEY ( service_id ) REFERENCES services( service_id );

ALTER TABLE services ADD CONSTRAINT fk_services FOREIGN KEY ( parent_service_id ) REFERENCES services( service_id );

ALTER TABLE task_definitions ADD CONSTRAINT fk_task_definitions FOREIGN KEY ( experiment ) REFERENCES experiments( experiment );

ALTER TABLE task_definitions ADD CONSTRAINT fk_task_definitions_creator FOREIGN KEY ( creator ) REFERENCES experimenters( experimenter_id );

ALTER TABLE task_definitions ADD CONSTRAINT fk_task_definitions_updater FOREIGN KEY ( updater ) REFERENCES experimenters( experimenter_id );

ALTER TABLE tasks ADD CONSTRAINT fk_tasks FOREIGN KEY ( campaign_id ) REFERENCES campaigns( campaign_id );

ALTER TABLE tasks ADD CONSTRAINT fk_tasks_waiting_for FOREIGN KEY ( waiting_for ) REFERENCES tasks( task_id );

ALTER TABLE tasks ADD CONSTRAINT fk_tasks_creator FOREIGN KEY ( creator ) REFERENCES experimenters( experimenter_id );

ALTER TABLE tasks ADD CONSTRAINT fk_tasks_updater FOREIGN KEY ( updater ) REFERENCES experimenters( experimenter_id );

