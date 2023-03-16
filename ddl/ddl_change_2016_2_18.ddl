
alter sequence task_definitions_task_definition_id_seq rename to campaign_definitions_campaign_definition_id_seq;

alter index pk_task_definitions rename to pk_campaign_definitions;
alter index idx_task_definitions_name rename to idx_campaign_definitions_name;
alter index idx_task_definitions_creator rename to idx_campaign_definitions_creator; 
alter index idx_task_definitions_experiment rename to idx_campaign_definitions_experiment;
alter index idx_task_definitions_updater rename to idx_campaign_definitions_updater;
ALTER INDEX idx_campaigns_task_definition_id RENAME TO idx_campaigns_campaign_definition_id;

alter table task_definitions RENAME CONSTRAINT fk_task_definitions to fk_campaign_definitions;
alter table task_definitions RENAME CONSTRAINT fk_task_definitions_creator to fk_campaign_definitions_creator;
alter table task_definitions RENAME CONSTRAINT fk_task_definitions_updater to fk_campaign_definitions_updater;

alter table task_definitions rename column task_definition_id to campaign_definition_id;

alter table task_definitions rename to campaign_definitions;

alter table campaigns rename column task_definition_id to campaign_definition_id;


CREATE TABLE launch_templates ( 
	launch_id            serial  NOT NULL,
	name                 text  NOT NULL,
	experiment           varchar(10)  NOT NULL,
	launch_host          text  NOT NULL,
	launch_account       text  NOT NULL,
	launch_setup         text  NOT NULL,
	creator              integer  NOT NULL,
	created              timestamptz  NOT NULL,
	updater              integer  ,
	updated              timestamptz  ,
	CONSTRAINT pk_launch_templates PRIMARY KEY ( launch_id )
 );

CREATE UNIQUE INDEX idx_launch_templates_experiment_name ON launch_templates ( experiment, name );

CREATE INDEX idx_launch_templates_creator ON launch_templates ( creator );

CREATE INDEX idx_launch_templates_updater ON launch_templates ( updater );

COMMENT ON COLUMN launch_templates.experiment IS 'Acroynm for the experiment';

ALTER TABLE launch_templates ADD CONSTRAINT fk_launch_templates_experiment FOREIGN KEY ( experiment ) REFERENCES experiments( experiment );

ALTER TABLE launch_templates ADD CONSTRAINT fk_launch_templates_creator FOREIGN KEY ( creator ) REFERENCES experimenters( experimenter_id );

ALTER TABLE launch_templates ADD CONSTRAINT fk_launch_templates_updater FOREIGN KEY ( updater ) REFERENCES experimenters( experimenter_id );


ALTER TABLE campaigns add launch_id integer;
CREATE INDEX idx_campaigns_launch_id ON campaigns ( launch_id );
ALTER TABLE campaigns ADD CONSTRAINT fk_campaigns_launch_id FOREIGN KEY ( launch_id ) REFERENCES launch_templates( launch_id );
ALTER INDEX idx_campaigns_0 RENAME TO idx_campaigns_updater

insert into launch_templates (experiment, launch_host, launch_account, launch_setup, creator, created)
  values ('nova', 'novagpvm02', 'novapro', 'setup yada-yada-yada', 5, now());
update campaigns set launch_id = (select launch_id from launch_templates);
alter table campaigns alter launch_id set not null;


ALTER TABLE campaigns ADD param_overrides json  ;

ALTER TABLE jobs ADD cpu_time float8  ;

ALTER TABLE jobs ADD wall_time float8  ;


CREATE TABLE tags ( 
	tag_id               serial  NOT NULL,
	experiment           varchar(10)  NOT NULL,
	tag_name             varchar(100)  NOT NULL,
	CONSTRAINT pk_tags PRIMARY KEY ( tag_id ),
	CONSTRAINT idx_tags_experiment_tag_name UNIQUE ( experiment, tag_name ) 
 );

CREATE INDEX idx_tags ON tags ( experiment );

ALTER TABLE tags ADD CONSTRAINT fk_tags_experiment FOREIGN KEY ( experiment ) REFERENCES experiments( experiment );


CREATE TABLE campaigns_tags ( 
	campaign_id          integer  NOT NULL,
	tag_id               integer  NOT NULL,
	CONSTRAINT pk_campaign_tags PRIMARY KEY ( campaign_id, tag_id )
 );

CREATE INDEX idx_campaigns_tags_tag_id ON campaigns_tags ( tag_id );

CREATE INDEX idx_campaigns_tags_campaign_id ON campaigns_tags ( campaign_id );

ALTER TABLE campaigns_tags ADD CONSTRAINT fk_campaigns_tags FOREIGN KEY ( tag_id ) REFERENCES tags( tag_id );

ALTER TABLE campaigns_tags ADD CONSTRAINT fk_campaigns_tags_0 FOREIGN KEY ( campaign_id ) REFERENCES campaigns( campaign_id );





grant select, insert, update, delete on all tables in schema public to pomsdbs;
grant usage on all sequences in schema public to pomsdbs;

