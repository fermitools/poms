
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

ALTER TABLE campaign_definitions ADD creator_role text  NOT NULL default 'production';
alter table campaign_definitions alter column creator_role drop default;

ALTER TABLE campaigns ADD creator_role text  NOT NULL default 'production';
alter table campaigns alter column creator_role drop default;

ALTER TABLE launch_templates ADD creator_role text  NOT NULL default 'production';
alter table launch_templates alter column creator_role drop default;

ALTER TABLE campaigns ADD role_held_with text;

ALTER TABLE experiments_experimenters DROP CONSTRAINT ck_role;

ALTER TABLE experiments_experimenters ADD CONSTRAINT
  ck_role CHECK (role = 'coordinator'::text or role = 'production'::text or role = 'analysis'::text);

ALTER TABLE experimenters ADD session_role text DEFAULT 'analysis' NOT NULL;

ALTER TABLE campaigns ADD campaign_type text NOT NULL default 'regular';
alter table campaigns alter column campaign_type drop default;

ALTER TABLE campaigns ADD CONSTRAINT ck_campaign_type CHECK ( (campaign_type = 'test'::text)
  or (campaign_type = 'data transfer'::text) or (campaign_type = 'keep up'::text) or (campaign_type = 'regular'::text) );

ALTER TABLE campaigns ADD CONSTRAINT ck_creator_role CHECK ( (creator_role = 'analysis'::text)
  or (creator_role = 'production'::text) );

ALTER TABLE campaign_definitions ADD CONSTRAINT ck_creator_role CHECK ( (creator_role = 'analysis'::text)
  or (creator_role = 'production'::text) );

ALTER TABLE tags ADD creator_role text  NOT NULL default 'production';
alter table tags alter column creator_role drop default;

CREATE INDEX idx_tags_created_by ON tags ( creator ) ;
ALTER TABLE tags ADD CONSTRAINT fk_tags_experimenters FOREIGN KEY ( creator ) REFERENCES experimenters( experimenter_id )  ;

ALTER TABLE tags ADD creator integer  NOT NULL default '5';
alter table tags alter column creator drop default;

ALTER TABLE tags ADD CONSTRAINT ck_creator_role
  CHECK ( (creator_role = 'analysis'::text) or (creator_role = 'production'::text) );

ALTER Table held_launches ADD launcher integer;

CREATE INDEX idx_held_launches_launcher ON held_launches ( launcher ) ;

CREATE INDEX idx_held_launches_campaign_id ON held_launches ( campaign_id ) ;

ALTER TABLE held_launches ADD CONSTRAINT fk_held_launches_experimenters FOREIGN KEY ( launcher ) REFERENCES experimenters( experimenter_id )  ;

ALTER TABLE held_launches ADD CONSTRAINT fk_held_launches_campaigns FOREIGN KEY ( campaign_id ) REFERENCES campaigns( campaign_id )  ;
