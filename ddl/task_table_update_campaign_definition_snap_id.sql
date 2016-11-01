
--Be sure all definitions have at least one snapshot
-- (at one time the code didn't copy)
with nosnap AS (
  select campaign_definition_id from campaign_definitions
  except
  select distinct campaign_definition_id from campaign_definition_snapshots
), snap AS (
  select lt.campaign_definition_id,lt.name,lt.experiment,lt.launch_script,lt.definition_parameters,
  	 lt.input_files_per_job,lt.output_files_per_job,lt.creator,lt.created,lt.updater,lt.updated,lt.output_file_patterns 
  from campaign_definitions lt, nosnap
  where lt.campaign_definition_id = nosnap.campaign_definition_id
)
insert into campaign_definition_snapshots
select nextval('campaign_definition_snapshots_campaign_definition_snap_id_seq'),
       snap.campaign_definition_id,snap.name,snap.experiment,snap.launch_script,snap.definition_parameters,
       snap.input_files_per_job,snap.output_files_per_job,snap.creator,snap.created,snap.updater,snap.updated,snap.output_file_patterns 
from snap;	  

-- Update all task_ids that use a campaign_definition having only one
-- record in the snapshot table.
with snap AS (
    select t.task_id,ls.campaign_definition_snap_id
    from campaigns c, tasks t,campaign_definition_snapshots ls
    where t.campaign_definition_snap_id is null
      and t.campaign_id = c.campaign_id
      and ls.campaign_definition_id = c.campaign_definition_id
      and c.campaign_definition_id in (select distinct campaign_definition_id 
                                       from campaign_definition_snapshots
                                       where name in (select name from campaign_definition_snapshots
                                         group by name
                                         having count(name) = 1)
                         )
)
update tasks t
  set campaign_definition_snap_id = snap.campaign_definition_snap_id
from snap
where t.task_id = snap.task_id;



-- Update all task_ids that use a campaign_definition having multiple
-- records in the snapshot table.
with task_data AS (
  select c.campaign_id, t.task_id, t.created, c.campaign_definition_id, l.name 
  from campaigns c, tasks t, campaign_definitions l
  where t.campaign_id = c.campaign_id
    and l.campaign_definition_id = c.campaign_definition_id
    and t.task_id in (select task_id from tasks where campaign_definition_snap_id is null)
), snap AS (
  select td.task_id, max(ls.campaign_definition_snap_id) campaign_definition_snap_id
  from campaign_definition_snapshots ls, task_data td
  where ls.name = td.name 
    and ls.updated <= td.created
  group by td.task_id
)
update tasks t
  set campaign_definition_snap_id = snap.campaign_definition_snap_id
from snap
where t.task_id = snap.task_id;


-- Some records were faked when the snapshot tables were added
-- to the schema, so just pick the min and fill it in.
with task_data AS (
  select c.campaign_id, t.task_id, t.created, c.campaign_definition_id, l.name 
  from campaigns c, tasks t, campaign_definitions l
  where t.campaign_id = c.campaign_id
    and l.campaign_definition_id = c.campaign_definition_id
    and t.task_id in (select task_id from tasks where campaign_definition_snap_id is null)
), snap AS (
  select td.task_id, min(ls.campaign_definition_snap_id) campaign_definition_snap_id
  from campaign_definition_snapshots ls, task_data td
  where ls.name = td.name 
  group by td.task_id
)
update tasks t
  set campaign_definition_snap_id = snap.campaign_definition_snap_id
from snap
where t.task_id = snap.task_id;
