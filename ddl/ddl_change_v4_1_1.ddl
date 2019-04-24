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
<<<<<<< HEAD
=======

insert into submission_statuses (status_id, status) values (10000, 'Awaiting Approval');
insert into submission_statuses (status_id, status) values (11000, 'Approved');
>>>>>>> ea9ed70... Added checkmark for approval - needs server side work.
