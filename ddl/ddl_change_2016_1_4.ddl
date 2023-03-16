

alter table campaigns alter task_definition_id drop default;
drop sequence campaigns_task_definition_id_seq;

alter table tasks alter campaign_id drop default;
drop sequence tasks_campaign_id_seq;

alter table tasks drop task_definition_id;
drop sequence tasks_task_definition_id_seq;

ALTER TABLE campaigns ADD cs_last_split timestamptz  ;
ALTER TABLE campaigns ADD cs_split_type text  ;
ALTER TABLE campaigns ADD cs_split_dimensions text  ;

ALTER TABLE campaigns ADD active bool DEFAULT true NOT NULL;

ALTER TABLE campaigns ADD dataset text;
update campaigns set dataset='unknown' where dataset is null;
alter table campaigns alter dataset set not null;

ALTER TABLE campaigns ADD software_version text;
update campaigns set software_version='unknown' where software_version is null;
alter table campaigns alter software_version set not null;



ALTER TABLE jobs ADD input_file_names text  ;
ALTER TABLE jobs ADD reason_held text;
ALTER TABLE jobs ADD consumer_id text;



ALTER TABLE experiments ADD CONSTRAINT idx_experiments_name UNIQUE ( name ) ;

ALTER TABLE experimenters ADD CONSTRAINT idx_experimenters_email UNIQUE ( email ) ;

insert into experiments values ('public', 'public');

insert into experiments_experimenters (experiment,experimenter_id)  
  (select 'public',experimenter_id from experimenters where experimenter_id not in(select experimenter_id from experiments_experimenters));


update jobs set cpu_type = substr(cpu_type,0,19)||0  where substr(cpu_type,19,1) in ('0','1','2','3','4');

update jobs set cpu_type = substr(cpu_type,0,14) || format('%s.0',((substr(cpu_type,14,4)||'0')::integer/10  + 1)) 
where cpu_type != 'unknown' and job_id > 1000 and substr(cpu_type,19,1) in ('5','6','7','8','9');



