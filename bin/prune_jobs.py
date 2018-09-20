#!/bin/env python

import argparse
import time
import datetime
import logging
import psycopg2

def parse_command_line():
    doc = "Deletes jobs data 3 weeks before current (runtime) date.  Deletion date can be changed"
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('host', help="Host database is on.")
    parser.add_argument('port', type=int, help="Database Port number.")
    parser.add_argument('dbname', help="Database to connect to.")
    parser.add_argument('user', help="User to connect as")
    parser.add_argument('-d', '--date',
        help="Data before this date will be rolled up and deleted. Default is 21 days before run date.")
    parser.add_argument('-p', '--password', help="Password for the database user account. For testing only, for production use .pgpass .")
    parser.add_argument('-v', '--verbose', action="store_true", help='Output log data to screen')
    parser.add_argument('-l', '--log_dir', help="Output directory for log file.")
    args = parser.parse_args()
    return args

def set_logging(log_dir, verbose):
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)
    if log_dir:
        log_file = "%s/rollup-%s.log" % (log_dir, time.strftime('%Y-%m-%d-%H%M%S'))
        fileHandler = logging.FileHandler(log_file)
        fileHandler.setFormatter(logFormatter)
        fileHandler.setLevel(logging.DEBUG)
        rootLogger.addHandler(fileHandler)
    if verbose:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        consoleHandler.setLevel(logging.DEBUG)
        rootLogger.addHandler(consoleHandler)

def get_deletion_date(date_text):
    if date_text is None:
        date_text = (datetime.datetime.now() - datetime.timedelta(days=21)).strftime("%Y-%m-%d")
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")
    return date_text

def get_task_id_list(cursor, date_text):
    sql = """select task_id from tasks where created < '%s'
             and task_id >= (select min(task_id) from jobs)
             order by task_id""" % date_text
    cursor.execute(sql)
    task_ids = cursor.fetchall()
    return task_ids

def run_sql(conn, cursor, sql, commit=False):
    try:
        #logging.debug("run_sql:SQL: %s", sql)
        cursor.execute(sql)
        if commit:
            conn.commit()
            #logging.debug("      rowcount: %s", cursor.rowcount)
    except psycopg2.Error as e:
        logging.debug("run_sql: EXCEPTION, SQL: %s", sql)
        logging.debug("run_sql:   Code: %s    Error: %s", e.pgcode, e.pgerror)
        raise e

def prune_jobs(conn, cursor, task_id):
    retValue = True
    sql = "select job_id from jobs where task_id=%s" % task_id
    try:
        run_sql(conn, cursor, sql, commit=False)
        job_ids = cursor.fetchall()
        for (job_id, ) in job_ids:
            del_files = "delete from job_files where job_id = %s" % job_id
            run_sql(conn, cursor, del_files, commit=True)
            del_histories = "delete from job_histories where job_id = %s" % job_id
            run_sql(conn, cursor, del_histories, commit=True)
        del_jobs = "delete from jobs where task_id = %s" % task_id
        run_sql(conn, cursor, del_jobs, commit=True)
    except psycopg2.Error as e:
        logging.debug("prune_jobs: skipping task_id = %s, moving on...", task_id)
        retValue = False
    return retValue

def main():

    stime = time.time()
    args = parse_command_line()
    set_logging(args.log_dir, args.verbose)

    password = ""
    if args.password:
        password = "password=%s" % args.password
    logging.debug("main:POMS Database: %s Host: %s Port:%s", args.dbname, args.host, args.port)

    conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s %s" % (args.dbname, args.host, args.port, args.user, password))
    cursor = conn.cursor()
    del_date = get_deletion_date(date_text=args.date)
    logging.debug("Deleting job data before %s.", del_date)

    task_ids = get_task_id_list(cursor, del_date)
    tasktot = len(task_ids)
    taskcnt = 0
    prtcnt = 0
    if tasktot:
        logging.debug("Pruning %s tasks.  From task_id %s to task_id %s", tasktot, task_ids[0][0], task_ids[tasktot-1][0])

    for (task_id, ) in task_ids:
        status = prune_jobs(conn, cursor, task_id)
        if not status:
            cursor.close()
            conn.close()
            conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s %s" % (args.dbname, args.host, args.port, args.user, password))
            cursor = conn.cursor()
            taskcnt -= 1
            logging.debug("main: skipped task_id: %s, reopened conn and cursor", task_id)
        taskcnt += 1
        prtcnt += 1
        if prtcnt == 10:
            logging.debug("Completed %s of %s task prunings.", taskcnt, tasktot)
            prtcnt = 0
    logging.debug("Pruned %s tasks", taskcnt)

    cursor.close()
    conn.close()
    logging.debug("elapsed seconds: %s", (time.time() - stime))
    logging.debug("finito!")

if __name__ == "__main__":
    main()
