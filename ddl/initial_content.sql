
insert into experiments (experiment, name) values ('root', 'root');

insert into experimenters (first_name, last_name, email) values ('Mike', 'Diesburg', 'diesburg@fnal.gov');
insert into experimenters (first_name, last_name, email) values ('Robert', 'Illingworth', 'illingwo@fnal.gov');
insert into experimenters (first_name, last_name, email) values ('Michael', 'Gheith', 'mgheith@fnal.gov');
insert into experimenters (first_name, last_name, email) values ('Marc', 'Mengel', 'mengel@fnal.gov');
insert into experimenters (first_name, last_name, email) values ('Stephen', 'White', 'swhite@fnal.gov');


insert into experiments_experimenters (experiment, experimenter_id, active) (select experiments.experiment, experimenters.experimenter_id, 'Y' 
									     from experiments CROSS JOIN experimenters);

