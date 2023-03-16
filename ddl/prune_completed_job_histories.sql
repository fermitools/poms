
with dd AS (
    select job_id, created
    from job_histories
    where status = 'Completed'
    except
    select job_id, max(created)
    from job_histories
    where status = 'Completed'
    group by job_id
) 
delete from job_histories j 
where j.status='Completed'
  and j.job_id in (select dd.job_id from dd)
  and j.created in (select  dd.created from dd)
;



