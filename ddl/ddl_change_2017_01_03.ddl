
alter experiments add column restricted boolean set default false;


ALTER TABLE campaigns ADD completion_type text DEFAULT 'located' NOT NULL;

ALTER TABLE campaigns ADD completion_pct integer DEFAULT 95 NOT NULL;

ALTER TABLE campaign_snapshots ADD completion_type text DEFAULT 'located' NOT NULL;

ALTER TABLE campaign_snapshots ADD completion_pct integer DEFAULT 95 NOT NULL;

ALTER TABLE campaigns ADD CONSTRAINT ch_completion_type CHECK ( (completion_type = 'located'::text) or (completion_type = 'complete'::text) ) ;

ALTER TABLE campaigns ADD CONSTRAINT ck_completion_pct CHECK ( (completion_pct > 0 and completion_pct <= 100) ) ;


CREATE TABLE held_launches ( 
	campaign_id          integer  NOT NULL,
	created              timestamptz  NOT NULL,
	parent_task_id       integer  ,
	dataset              text  ,
	CONSTRAINT pk_held_launches PRIMARY KEY ( campaign_id, created )
 ) ;

