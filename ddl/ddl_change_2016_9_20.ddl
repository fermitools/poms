
ALTER TABLE campaign_definitions DROP CONSTRAINT idx_campaign_definitions_name;
CREATE INDEX idx_campaign_definitions_name ON campaign_definitions ( name ) ;

DROP INDEX idx_campaign_definitions_experiment;
ALTER TABLE campaign_definitions ADD CONSTRAINT idx_campaign_definitions_experiment UNIQUE ( experiment, name );

