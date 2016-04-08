
CREATE TABLE output_files ( 
	job_id               bigint  NOT NULL,
	file_name            text  NOT NULL,
	created              timestamptz  NOT NULL,
	declared             timestamptz  ,
	CONSTRAINT pk_output_files PRIMARY KEY ( job_id, file_name )
 ) ;

ALTER TABLE output_files ADD CONSTRAINT fk_output_files FOREIGN KEY ( job_id ) REFERENCES jobs( job_id )    ;








grant select, insert, update, delete on all tables in schema public to pomsdbs;
grant usage on all sequences in schema public to pomsdbs;
