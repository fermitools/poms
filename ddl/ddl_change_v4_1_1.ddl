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

ALTER TABLE campaign_stages ADD merge_overrides bool DEFAULT false;
