
--Be sure all layouts have at least one snapshot
-- (at one time the code didn't copy)
with nosnap AS (
  select launch_id from launch_templates
  except
  select distinct launch_id from launch_template_snapshots
), snap AS (
  select lt.launch_id,lt.experiment,lt.launch_host,lt.launch_account,lt.launch_setup,
       lt.creator,lt.created,lt.updater,lt.updated,lt.name
  from launch_templates lt, nosnap
  where lt.launch_id = nosnap.launch_id
)
insert into launch_template_snapshots
select nextval('launch_template_snapshots_launch_snapshot_id_seq'),
       snap.launch_id,snap.experiment,snap.launch_host,snap.launch_account,snap.launch_setup,
       snap.creator,snap.created,snap.updater,snap.updated,snap.name
from snap;	  


-- Update all task_ids that use a launch_template that has only one
-- record in the snapshot table.
with snap AS (
    select t.task_id,ls.launch_snapshot_id
    from campaigns c, tasks t,launch_template_snapshots ls
    where t.launch_snapshot_id is null
      and t.campaign_id = c.campaign_id
      and ls.launch_id = c.launch_id
      and c.launch_id in (select distinct launch_id 
                          from launch_template_snapshots
                          where name in (select name from launch_template_snapshots
                                         group by name
                                         having count(name) = 1)
                         )
)
update tasks t
  set launch_snapshot_id = snap.launch_snapshot_id
from snap
where t.task_id = snap.task_id;


-- Update all task_ids that use a launch_template that has multiple
-- records in the snapshot table.
with task_data AS (
  select c.campaign_id, t.task_id, t.created, c.launch_id, l.name 
  from campaigns c, tasks t, launch_templates l
  where t.campaign_id = c.campaign_id
    and l.launch_id = c.launch_id
    and t.task_id in (select task_id from tasks where launch_snapshot_id is null)
), snap AS (
select td.task_id, max(ls.launch_snapshot_id) launch_snapshot_id
from launch_template_snapshots ls, task_data td
where ls.name = td.name 
  and ls.updated <= td.created
group by td.task_id
)
update tasks t
  set launch_snapshot_id = snap.launch_snapshot_id
from snap
where t.task_id = snap.task_id;


-- Some records were faked when the snapshot tables were added
-- to the schema, so just pick the min and fill it in.
with task_data AS (
  select c.campaign_id, t.task_id, t.created, c.launch_id, l.name 
  from campaigns c, tasks t, launch_templates l
  where t.campaign_id = c.campaign_id
    and l.launch_id = c.launch_id
    and t.task_id in (select task_id from tasks where launch_snapshot_id is null)
), snap AS (
select td.task_id, min(ls.launch_snapshot_id) launch_snapshot_id
from launch_template_snapshots ls, task_data td
where ls.name = td.name 
group by td.task_id
)
update tasks t
  set launch_snapshot_id = snap.launch_snapshot_id
from snap
where t.task_id = snap.task_id;
