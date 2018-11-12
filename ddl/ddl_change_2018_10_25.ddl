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

-- Create a copy of this table...just in case!
create table submissiion_histories_old as (select * from submission_histories);

CREATE TABLE submission_statuses (
	status_id            integer  NOT NULL ,
	status               text  NOT NULL ,
	CONSTRAINT pk_submission_statuses_status_id PRIMARY KEY ( status_id )
 );
insert into submission_statuses (status_id, status) values (1000, 'New');
insert into submission_statuses (status_id, status) values (2000, 'Launch Failed');
insert into submission_statuses (status_id, status) values (3000, 'Idle');
insert into submission_statuses (status_id, status) values (4000, 'Running');
insert into submission_statuses (status_id, status) values (5000, 'Held');
insert into submission_statuses (status_id, status) values (6000, 'Failed');
insert into submission_statuses (status_id, status) values (7000, 'Completed');
insert into submission_statuses (status_id, status) values (8000, 'Located');
insert into submission_statuses (status_id, status) values (9000, 'Removed');

ALTER TABLE submission_histories ADD COLUMN status_id integer;

update submission_histories set status_id=1000 where status in('new', 'New');
update submission_histories set status_id=2000 where status in('Launch Failed', 'LaunchFailed');
update submission_histories set status_id=3000 where status = 'Idle';
update submission_histories set status_id=4000 where status = 'Running';
update submission_histories set status_id=5000 where status = 'Held';
update submission_histories set status_id=6000 where status = 'Failed';
update submission_histories set status_id=7000 where status = 'Completed';
update submission_histories set status_id=8000 where status = 'Located';

delete from submission_histories where status in('started', 'testing', 'unknown', 'Unknown');

--- Do not run these in DEV until Marc is ready for them

ALTER TABLE submission_histories ALTER COLUMN status_id set NOT NULL;

ALTER TABLE submission_histories ADD CONSTRAINT fk_submission_histories_submission_statuses FOREIGN KEY ( status_id )
  REFERENCES submission_statuses( status_id )    ;

-- After verifying run the following
-- ALTER TABLE submission_histories drop column status;
