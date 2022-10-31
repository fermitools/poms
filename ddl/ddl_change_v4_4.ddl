alter table campaign_stages add column default_clear_cronjob bool default true;
alter table campaign_stage_snapshots add column default_clear_cronjob bool default true;

Update campaign_stages set default_clear_cronjob = false;
Update campaign_stage_snapshots set default_clear_cronjob = false;

insert into submission_statuses (status_id, status) values (1500, 'Cancelled');
