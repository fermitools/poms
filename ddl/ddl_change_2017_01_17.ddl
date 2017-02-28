

ALTER TABLE experimenters ADD last_login timestamptz DEFAULT now() NOT NULL;

% indexes added to help speed up dev db, should be in prod too
create index concurrently job_files_file_name_idx on job_files ( file_name ) ;
create index concurrently idx_jobs_by_jobsub_job_id on jobs ( jobsub_job_id ) ;
create index concurrently idx_tasks_by_status on tasks ( status ) ;

% add column to primary key
Alter table campaign_recoveries drop constraint pk_campaign_recoveries;
ALTER TABLE campaign_recoveries ADD CONSTRAINT pk_campaign_recoveries PRIMARY KEY ( campaign_definition_id, recovery_type_id, recovery_order );


CREATE TABLE faulty_requests ( 
	url                  text  NOT NULL,
	last_seen            timestamptz DEFAULT now() NOT NULL,
	status               integer  ,
	message              text  ,
	ntries               integer  ,
	CONSTRAINT faulty_requests_pkey PRIMARY KEY ( url, last_seen )
 );

