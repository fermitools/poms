ALTER TABLE campaign_stages DROP CONSTRAINT ck_campaign_stage_type;
ALTER TABLE campaign_stages ADD CONSTRAINT ck_campaign_stage_type
  CHECK ( campaign_stage_type::text = ANY (ARRAY['test'::character varying, 'generator'::character varying, 'approval'::character varying, 'regular'::character varying, 'datatransfer'::character varying]::text[])  );

