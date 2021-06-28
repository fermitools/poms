
ALTER TABLE campaign_stages ADD test_split_type text  DEFAULT '';

ALTER TABLE submissions ADD files_consumed integer DEFAULT NULL;
ALTER TABLE submissions ADD files_generated integer DEFAULT NULL;

