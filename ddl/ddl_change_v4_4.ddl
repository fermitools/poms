alter table campaign_stages add column default_clear_cronjob bool default true;
alter table campaign_stage_snapshots add column default_clear_cronjob bool default true;

Update campaign_stages set default_clear_cronjob = false;
Update campaign_stage_snapshots set default_clear_cronjob = false;

insert into submission_statuses (status_id, status) values (1500, 'Cancelled');

CREATE TABLE experimenters_watching ( 
	experimenters_watching_id uuid  NOT NULL,
	experimenter_id           int  NOT NULL,
	campaign_id               int  NOT NULL,
	created              timestamptz  NOT NULL,
	CONSTRAINT pk_experimenters_watching PRIMARY KEY ( experimenters_watching_id ),
	CONSTRAINT fk_experimenter_id FOREIGN KEY ( experimenter_id ) REFERENCES experimenters( experimenter_id ),
	CONSTRAINT fk_campaign_id FOREIGN KEY ( campaign_id ) REFERENCES campaigns( campaign_id ) 
 );

alter table experimenters_watching OWNER TO pomsdev;


update recovery_types set name = 'process_status' where name = 'proj_status';

alter table campaign_stages alter column completion_pct type real;
alter table campaign_stage_snapshots alter column completion_pct type real;

\i grants.sql
