
CREATE OR REPLACE FUNCTION update_job_history() RETURNS TRIGGER AS $update_job_history$
BEGIN
   
    IF TG_OP = 'INSERT' or  NEW.status != OLD.status THEN
        INSERT INTO job_histories SELECT NEW.job_id, now(), NEW.status;
    END IF;
    RETURN NULL;
END;
$update_job_history$ LANGUAGE plpgsql;


CREATE TRIGGER update_job_history 
  AFTER INSERT OR UPDATE on jobs
   FOR EACH ROW EXECUTE PROCEDURE update_job_history();

CREATE OR REPLACE FUNCTION update_task_history() RETURNS TRIGGER AS $update_task_history$
BEGIN
    IF TG_OP = 'INSERT' or NEW.status != OLD.status THEN
       INSERT INTO task_histories SELECT NEW.task_id, now(), NEW.status;
    END IF;
    RETURN NULL;
END;
$update_task_history$ LANGUAGE plpgsql;

CREATE TRIGGER update_task_history 
  AFTER INSERT OR UPDATE on tasks
   FOR EACH ROW EXECUTE PROCEDURE update_task_history();
