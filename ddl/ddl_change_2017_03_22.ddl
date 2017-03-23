

alter table experimenters add username text;
create unique index idx_expermenters_username on experimenters (username);
alter table experimenters alter column email drop not null;


delete from experiments_experimenters where experimenter_id in(select experimenter_id from experimenters where email not like '%@fnal.gov');
delete from experimenters where email not like '%@fnal.gov';
update experimenters set username=split_part(email,'@',1);


