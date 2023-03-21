
DROP TABLE campaign_dependencies;

CREATE TABLE campaign_dependencies ( 
	campaign_dep_id       serial  NOT NULL,
        needs_camp_id         integer NOT NULL,
        uses_camp_id          integer NOT NULL,
	file_patterns         text    NOT NULL,
	CONSTRAINT pk_campaign_dependencies PRIMARY KEY ( campaign_dep_id )
);

ALTER TABLE campaign_dependencies ADD CONSTRAINT fk_campaign_dependencies_up
 FOREIGN KEY ( needs_camp_id ) REFERENCES campaigns( campaign_id );
ALTER TABLE campaign_dependencies ADD CONSTRAINT fk_campaign_dependencies_down
 FOREIGN KEY ( uses_camp_id ) REFERENCES campaigns( campaign_id );
