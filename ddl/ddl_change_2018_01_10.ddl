
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

ALTER TABLE experimenters ADD active_role text DEFAULT 'analysis' NOT NULL;



ALTER TABLE campaigns ADD campaign_type text NOT NULL default 'regular';
alter table campaigns alter column campaign_type drop default;

ALTER TABLE campaigns ADD CONSTRAINT ck_campaign_type CHECK ( (campaign_type = 'test'::text)
  or (campaign_type = 'data transfer'::text) or (campaign_type = 'keep up'::text) or (campaign_type = 'regular'::text) );

ALTER TABLE campaigns ADD CONSTRAINT ck_creator_role CHECK ( (creator_role = 'analysis'::text)
  or (creator_role = 'production'::text) );
