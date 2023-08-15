ALTER TABLE campaigns ADD COLUMN data_handling_service TEXT NOT NULL DEFAULT 'sam';
ALTER TABLE campaign_stages ADD COLUMN data_dispatcher_dataset_query TEXT DEFAULT NULL;
ALTER TABLE campaign_stages ADD COLUMN data_dispatcher_project_id int DEFAULT NULL;
ALTER TABLE campaign_stage_snapshots ADD COLUMN data_dispatcher_dataset_query TEXT DEFAULT NULL;
ALTER TABLE campaign_stage_snapshots ADD COLUMN data_dispatcher_project_id int DEFAULT NULL;

CREATE SEQUENCE data_dispatcher_project_idx_seq;
ALTER SEQUENCE data_dispatcher_project_idx_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE data_dispatcher_submissions ( 
	data_dispatcher_project_idx         serial NOT NULL,
    project_id                          integer NOT NULL,   
    project_name                        text  NOT NULL,
    experiment                          text  NOT NULL,
    vo_role                             text NOT NULL,
    campaign_id                         integer  NOT NULL,  
    campaign_stage_id                   integer  DEFAULT NULL,
    campaign_stage_snapshot_id          integer  DEFAULT NULL,
    submission_id                       integer  DEFAULT NULL,                  
    split_type                          text  DEFAULT NULL,
    last_split                          integer  DEFAULT NULL,
    depends_on_submission               integer  DEFAULT NULL,
    recovery_type_id                    integer  DEFAULT NULL,
    recovery_tasks_parent_submission    integer  DEFAULT NULL,
    recovery_position                   integer  DEFAULT NULL,
    job_type_snapshot_id                integer  DEFAULT NULL,
    creator                             integer  NOT NULL,
	created                             timestamptz  DEFAULT NOW(),
	updater                             integer  DEFAULT NULL,
	updated                             timestamptz  DEFAULT NULL,
    worker_timeout                      integer DEFAULT NULL,
    idle_timeout                        integer DEFAULT NULL,
    active                              bool DEFAULT true NOT NULL,
    depends_on_project                  integer  DEFAULT NULL,
    recovery_tasks_parent_project       integer  DEFAULT NULL,
    jobsub_job_id                       text DEFAULT NULL,
    named_dataset                       text DEFAULT NULL,
    status                              text DEFAULT NULL,
    
	CONSTRAINT pk_data_dispatcher_projects PRIMARY KEY ( data_dispatcher_project_idx ),
	CONSTRAINT fk_dd_projects_experimenters FOREIGN KEY ( experiment ) REFERENCES experiments( experiment ),
    CONSTRAINT fk_dd_projects_campaigns FOREIGN KEY ( campaign_id ) REFERENCES campaigns ( campaign_id ),
    CONSTRAINT fk_dd_projects_campaign_stages FOREIGN KEY ( campaign_stage_id  ) REFERENCES campaign_stages ( campaign_stage_id );
    CONSTRAINT fk_dd_projects_campaign_stage_snapshots FOREIGN KEY ( campaign_stage_snapshot_id ) REFERENCES campaign_stage_snapshots ( campaign_stage_snapshot_id );
    CONSTRAINT fk_dd_projects_job_type_snapshots FOREIGN KEY ( job_type_snapshot_id ) REFERENCES job_type_snapshots ( job_type_snapshot_id );
    CONSTRAINT fk_dd_projects_submissions FOREIGN KEY ( submission_id ) REFERENCES submissions ( submission_id );
    CONSTRAINT fk_dd_projects_depends_on_submission FOREIGN KEY ( depends_on_submission ) REFERENCES submissions ( submission_id );
    CONSTRAINT fk_dd_projects_recovery_tasks_parent_submission FOREIGN KEY ( recovery_tasks_parent_submission ) REFERENCES submissions ( submission_id );
    CONSTRAINT fk_dd_projects_creator FOREIGN KEY ( creator ) REFERENCES experimenters ( experimenter_id );
    CONSTRAINT fk_dd_projects_updater FOREIGN KEY ( updater ) REFERENCES experimenters ( experimenter_id );
    CONSTRAINT fk_dd_projects_depends_on_project FOREIGN KEY ( depends_on_project ) REFERENCES data_dispatcher_projects ( data_dispatcher_project_idx );
    CONSTRAINT fk_dd_projects_recovery_tasks_parent_project FOREIGN KEY ( recovery_tasks_parent_project ) REFERENCES data_dispatcher_projects ( data_dispatcher_project_idx );
 );

alter table submissions add column data_dispatcher_project_idx int default null;
alter table submissions add CONSTRAINT fk_submission_dd_project FOREIGN KEY ( data_dispatcher_project_idx ) REFERENCES data_dispatcher_submissions( data_dispatcher_project_idx );

ALTER TABLE data_dispatcher_projects OWNER TO pomsdev;
grant select, insert, update, delete on data_dispatcher_projects to pomsdbs;
grant usage on all sequences in schema public to pomsdbs;