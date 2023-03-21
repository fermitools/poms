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

-- begin

ALTER TABLE campaign_stages ADD output_ancestor_depth integer DEFAULT 1  ;

ALTER TABLE campaigns ADD campaign_keywords json   ;

ALTER TABLE job_types ADD active bool DEFAULT true  ;

ALTER TABLE login_setups ADD active bool DEFAULT true  ;
