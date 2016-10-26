


--Be sure all campaigns to have at least one snapshot
-- (at one time the code didn't copy)
with nosnap AS (
  select campaign_id from campaigns
  except
  select distinct campaign_id from campaign_snapshots
), snap AS (
  select lt.campaign_id,lt.experiment,lt.campaign_definition_id,lt.creator,lt.created,lt.updater,lt.updated,lt.name,
  	 lt.vo_role,lt.cs_last_split,lt.cs_split_type,lt.cs_split_dimensions,lt.dataset,lt.software_version,lt.active,lt.launch_id,lt.param_overrides
  from campaigns lt, nosnap
  where lt.campaign_id = nosnap.campaign_id
)
insert into campaign_snapshots
select nextval('campaign_snapshots_campaign_snapshot_id_seq'),
       snap.campaign_id,snap.experiment,snap.name,snap.campaign_definition_id,snap.vo_role,snap.creator,
       snap.created,snap.active,snap.dataset,snap.software_version,snap.launch_id,snap.param_overrides,snap.updater,
       snap.updated,snap.cs_last_split,snap.cs_split_type,snap.cs_split_dimensions
from snap;



-- Update all task_ids using a campaign having only one
-- record in the snapshot table.
with snap AS (
    select t.task_id,ls.campaign_snapshot_id
    from campaigns c, tasks t,campaign_snapshots ls
    where t.campaign_snapshot_id is null
      and t.campaign_id = c.campaign_id
      and ls.campaign_id = c.campaign_id
      and c.campaign_id in (select distinct campaign_id 
                              from campaign_snapshots
                              where name in (select name from campaign_snapshots
                                             group by name
                                             having count(name) = 1)
                         )
)
update tasks t
  set campaign_snapshot_id = snap.campaign_snapshot_id
from snap
where t.task_id = snap.task_id;



-- Update all task_ids that use a campaign having multiple
-- records in the snapshot table.
with task_data AS (
  select c.campaign_id, t.task_id, t.created, c.name 
  from campaigns c, tasks t
  where t.campaign_id = c.campaign_id
    and t.task_id in (select task_id from tasks where campaign_snapshot_id is null)
), snap AS (
  select td.task_id, max(ls.campaign_snapshot_id) campaign_snapshot_id
  from campaign_snapshots ls, task_data td
  where ls.name = td.name 
    and ls.updated <= td.created
  group by td.task_id
)
update tasks t
  set campaign_snapshot_id = snap.campaign_snapshot_id
from snap
where t.task_id = snap.task_id;


-- Some records were faked when the snapshot tables were added
-- to the schema, so just pick the min and fill it in.
with task_data AS (
  select c.campaign_id, t.task_id, t.created, c.name 
  from campaigns c, tasks t
  where t.campaign_id = c.campaign_id
    and t.task_id in (select task_id from tasks where campaign_snapshot_id is null)
), snap AS (
  select td.task_id, min(ls.campaign_snapshot_id) campaign_snapshot_id
  from campaign_snapshots ls, task_data td
  where ls.name = td.name 
  group by td.task_id
)
update tasks t
  set campaign_snapshot_id = snap.campaign_snapshot_id
from snap
where t.task_id = snap.task_id;
