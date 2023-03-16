
ALTER TABLE campaign_snapshots ALTER COLUMN cs_last_split TYPE integer USING (cast(extract(epoch from cs_last_split) as integer));
ALTER TABLE campaigns ALTER COLUMN cs_last_split TYPE integer USING (cast(extract(epoch from cs_last_split) as integer));
