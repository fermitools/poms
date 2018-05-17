#!/bin/env python

import argparse
import time
import datetime
import logging
import psycopg2

def parse_command_line():
    doc = "Rolls up and deletes task/jobs data."
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('host', help="Host database is on.")
    parser.add_argument('port', type=int, help="Database Port number.")
    parser.add_argument('dbname', help="Database to connect to.")
    parser.add_argument('user', help="User to connect as")
    parser.add_argument('-d', '--date',
        help="Data THREE weeks before this date will be rolled up and deleted.  Defaults to the Monday three weeks before today.")
    parser.add_argument('-c', '--commit', action="store_true", help='Save changes to the database. Required to save data!')
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

def get_monday(cursor, date):
    if date is None:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
        logging.debug(date)
    sql = "select date_trunc('week', '%s'::date )::date" % date
    cursor.execute(sql)
    monday = cursor.fetchone()[0]
    return monday.strftime("%Y-%m-%d")

def get_task_id_list(cursor, monday):
    sql = "select task_id from tasks where created < '%s'" % monday
    print(sql)
    cursor.execute(sql)
    task_ids = cursor.fetchall()
    return task_ids

def prune_jobs(cursor, task_id):
    sql = "select job_id from jobs where task_id=%s" % task_id
    cursor.execute(sql)
    job_ids = cursor.fetchall()
    for (job_id, ) in job_ids:
        del_files = "delete job_files where job_id = %s" % job_id
        cursor.execute(del_files)
        cursor.commit()
        del_histories = "delete job_histories where job_id = %s" % job_id
        cursor.execute(del_histories)
        cursor.commit()
    del_jobs = "delete jobs where task_id = %s" % task_id
    cursor.execute(del_jobs)
    cursor.commit()

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
    monday = get_monday(cursor, date=args.date)
    logging.debug("Rolling up data before %s.", monday)
    task_ids = get_task_id_list(cursor, monday)
    for (task_id, ) in task_ids:
        prune_jobs(cursor, task_id)

    cursor.close()
    conn.close()
    logging.debug("main: elapsed seconds: %s", (time.time() - stime))
    logging.debug("main: finito!")

if __name__ == "__main__":
    main()
