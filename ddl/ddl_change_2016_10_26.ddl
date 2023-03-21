

ALTER TABLE tasks ALTER COLUMN launch_snapshot_id SET NOT NULL;
ALTER TABLE tasks ALTER COLUMN campaign_snapshot_id SET NOT NULL;
ALTER TABLE tasks ALTER COLUMN campaign_definition_snap_id SET NOT NULL;

