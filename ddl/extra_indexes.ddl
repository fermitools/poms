% indexes added to help speed up dev db, should be in prod too
create index concurrently job_files_file_name_idx on job_files ( file_name ) ;
create index concurrently idx_jobs_by_jobsub_job_id on jobs ( jobsub_job_id ) ;
create index concurrently idx_tasks_by_status on tasks ( status ) ;
