ALTER TABLE campaign_stages DROP CONSTRAINT ch_completion_type;
ALTER TABLE campaign_stages ADD CONSTRAINT ch_completion_type CHECK ( (completion_type = 'located'::text) or (completion_type = 'complete'::text) or (completion_type = 'strong_dd'::text) ) ;
