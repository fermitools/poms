
alter table experimenters add column session_experiment text;

delete from experiments_experimenters where experiment='public';
delete from experiment where experiment='public';

update experimenters
  set session_experiment=subquery.experiment
from (
     select distinct on (experimenter_id) experimenter_id, experiment
     from  experiments_experimenters
     where experiment not in('public','root')
     ) subquery
where experimenters.experimenter_id=subquery.experimenter_id;

delete from experimenters
where experimenter_id in (
   select experimenter_id from experimenters where experimenter_id not in(
      select experimenter_id from experiments_experimenters));

update experimenters set session_experiment='nova' where session_experiment is null;

alter table experimenters alter column session_experiment set not null;

CREATE OR REPLACE FUNCTION public.update_job_history()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'INSERT' or  NEW.status != OLD.status THEN
        INSERT INTO job_histories SELECT NEW.job_id, clock_timestamp(), NEW.status;
    END IF;
    RETURN NULL;
END;
$function$;


CREATE OR REPLACE FUNCTION public.update_task_history()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'INSERT' or NEW.status != OLD.status THEN
       INSERT INTO task_histories SELECT NEW.task_id, clock_timestamp(), NEW.status;
    END IF;
    RETURN NULL;
END;
$function$


-- end
