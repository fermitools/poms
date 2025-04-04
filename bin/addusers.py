#!/bin/env python

import argparse
import time
import logging
import smtplib
import json

import psycopg2
import requests
import os
from configparser import ConfigParser

config = ConfigParser()
config.read(os.environ["WEB_CONFIG"])

try:
    import urllib3 as urllib3
except ImportError:
    import urllib3

urllib3.disable_warnings()

def parse_command_line():
    doc = "Loads a VOMS generated JSON file of experimenters into POMS."
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('host', help="Host database is on.")
    parser.add_argument('port', type=int, help="Database Port number.")
    parser.add_argument('dbname', help="Database to connect to.")
    parser.add_argument('user', help="User to connect as")
    parser.add_argument('cert', help="Location of certificates.")
    parser.add_argument('-c', '--commit', action="store_true", help='Save changes to the database. Required to save data!')
    parser.add_argument('-f', '--ferry', help='FERRY url to use.')
    parser.add_argument('-s', '--skip_analysis', action="store_true", help='Do not include analysis users.')
    parser.add_argument('-e', '--experiment', help='Run for a specific experiment, otherwise runs for all experiments in database.')
    parser.add_argument('-p', '--password', help="Password for the database user account. For testing only, for production use .pgpass .")
    parser.add_argument('-v', '--verbose', action="store_true", help='Output log data to screen')
    parser.add_argument('-l', '--log_dir', help="Output directory for log file.")
    parser.add_argument('-L', '--list_owner', help="Name of a owner of poms_announce for listserv conformation emails.")
    args = parser.parse_args()
    return args

def add_to_listserv(list_owner, new_users):

    if list_owner is None:
        logging.debug("add_to_listserv - list_owner was not set")
        return
    if not new_users:
        logging.debug("add_to_listserv: No new users.")
        return

    listval = 'poms_announce'
    smtp_server = config.get("smtp", "smtp_host")
    fromAddr = '%s' % list_owner
    toAddr = config.get("global", "listserv_server")

    subject = "Add new_users to %s" %(listval)

    msg = ''
    for username, user in new_users.items():
        email = "%s@fnal.gov" % username
        msg += "add %s %s %s\r\n" %(listval, email, user['commonname'])
    message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s\r\n" % (fromAddr, toAddr, subject, msg)

    logging.debug("add_to_listserv - %s new users, updating %s", len(new_users), listval)
    logging.debug(message)
    logging.debug(msg)
    server = smtplib.SMTP(smtp_server)
    server.sendmail(fromAddr, [toAddr], message)
    server.quit()

def get_experiments(cursor, experiment):
    exp_list = []
    where_clause = "experiment not in ('samdev','root')"
    if experiment:
        where_clause = "experiment = '%s'" % experiment
    cursor.execute("select experiment from experiments where %s" % where_clause)
    for row in cursor.fetchall():
        exp_list.append(row[0])
    return exp_list

def query_ferry(cert, ferry_url, exp, role):
    results = []
    logging.debug("query_ferry for experiment: %s  role: %s", exp, role)
    url = ferry_url + "/getAffiliationMembersRoles?unitname=%s&role=%s" % (exp, role)
    logging.debug("query_ferry: requested url: %s", url)
    r = requests.get(url, verify=False, cert=('%s/pomscert.pem' % cert, '%s/pomskey.pem' % cert))
    payload = r.json()
    if r.status_code != requests.codes.get('ok'):
        logging.debug("get_ferry_experiment_users -- error status_code: %s  -- %s", r.status_code, url)
    elif payload.get('ferry_status') != 'success':
        logging.debug("get_ferry_experiment_users -- ferry_error: %s  -- %s", payload.get('ferry_error'), url)
    else:
        results = payload.get('ferry_output', [])[exp]
    return results

def query_superusers(cert, ferry_url, exp, anal_users):
    results = []
    url = ferry_url + "/getGroupMembers?grouptype=BatchSuperusers&groupname=%s" % (exp)
    r = requests.get(url, verify=False, cert=('%s/pomscert.pem' % cert, '%s/pomskey.pem' % cert))
    payload  = r.json()
    if r.status_code != requests.codes.get('ok'):
        logging.debug("get_ferry_experiment_users -- error status_code: %s  -- %s", r.status_code, url)
    elif payload.get('ferry_status') != 'success':
        logging.debug("query_superusers -- ferry_error: %s  -- %s", payload.get('ferry_error'), url)
    else:
        results = payload.get('ferry_output')
    return results

def get_ferry_data(cert, ferry_url, exp, skip_analysis):
    """
    Query Ferry for exps users by Analysis and Production.  Join them in one dictionary.
      Dict Returned:
      {username: {'commonname':  'value',  'role': 'value'},
                 {'commonname':  'value',  'role': 'value'},
      }
    """
    users = {}
    anal_users = {}
    su_users = {}
    if skip_analysis is False:
        anal_users = query_ferry(cert, ferry_url, exp, 'Analysis')
        su_users = query_superusers(cert, ferry_url, exp, anal_users)
    prod_users = query_ferry(cert, ferry_url, exp, 'Production')

    for anal in anal_users:
        users[anal.get('username')] = {'commonname': anal.get('fullname'), 'role': 'analysis'}
    for prod in prod_users:
        users[prod.get('username')] = {'commonname': prod.get('fullname'), 'role': 'production'}
    for su in su_users:
        users[su.get('username')] = {'commonname': su.get('fullname'), 'role': 'superuser'}

    #logging.debug("ferry data: %s", str(users))
    return users

def get_voms_data(cert, exp):
    logging.debug("get_voms_data for: %s", exp)
    payload = {"accountName": "%spro" % exp}
    req = requests.get(config.get("VOMS", "data_url"), params=payload, verify=False, cert=('%s/pomscert.pem' % cert, '%s/pomskey.pem' % cert))
    users = {}
    for line in req.iter_lines():
        line = line.decode('utf-8')
        if line.find("CN=UID:") != -1:
            username = line.split("/CN=")[-1][4:]
            commonname = line.split("/CN=")[-2]
            # build a dictionary to get rid of duplicates, make it the
            # same structure as get_ferry_data builds
            users[username] = {'commonname': commonname, 'role': 'production'}
    logging.debug("voms data: %s", str(users))
    return users

def determine_changes(cursor, exp, users):
    sql = """
        select e.username, e2e.experimenter_id, e2e.active, e2e.role
        from experiments_experimenters e2e, experimenters e
        where e2e.experimenter_id = e.experimenter_id and
              e2e.experiment = '%s'
    """ % exp
    new_users = users.copy() # existing users will be popped off.
    active_status = {}
    role_changes = {}

    if not users:
        logging.warning("deterimine_changes: Zero users returned for experiment!  Skipping this experiment.")
        return {}, {}, {}

    cursor.execute(sql)
    for (username, experimenter_id, active, role,) in cursor.fetchall():
        # for purposes of this script, users who are production-shifters
        # have analysis role, as Ferry doesn't track production-shifters.
        if role == 'production-shifter':
            role = 'analysis'
        user = new_users.pop(username, None)
        if user is None:
            if active is True:
                active_status[username] = {'experimenter_id' : experimenter_id, 'active': False, 'username': username}
        elif user['role'] != role:
            role_changes[username] = {'experimenter_id': experimenter_id, 'role': user['role']}
        elif active is False:
            active_status[username] = {'experimenter_id' : experimenter_id, 'active': True, 'username': username}
    return new_users, active_status, role_changes

def add_users(cursor, exp, new_users):
    for username, user in new_users.items():
        sql = "select experimenter_id from experimenters where username='%s'" % username
        cursor.execute(sql)
        (experimenter_id,) = cursor.fetchone() or (None,)
        if experimenter_id is None:
            # should dump this stuff and use a single field in poms called commonname
            last_name = user['commonname']
            first_name = ""
            names = last_name.lstrip().split(" ", 1)
            if len(names) == 2:
                (first_name, last_name) = last_name.lstrip().split(" ", 1)
                first_name = first_name.replace("'", "''")
                last_name = last_name.replace("'", "''")
            sql = """
                insert into experimenters (last_name,first_name,username,session_experiment)
                    values ('%s','%s','%s','%s') returning experimenter_id
            """ % (last_name, first_name, username, exp)
            cursor.execute(sql)
            experimenter_id = cursor.fetchone()[0]
            logging.debug("add_user: inserted new experimenter id=%s username: %s commonname: %s ", experimenter_id, username, user['commonname'])
        add_relationship(cursor, experimenter_id, exp, user['role'], username)

def add_relationship(cursor, experimenter_id, exp, role, username):
    sql = "select 1 from experiments_experimenters where experiment = '%s' and experimenter_id = %s" % (exp, experimenter_id)
    cursor.execute(sql)
    if cursor.fetchone() is None:
        sql = "insert into experiments_experimenters (experiment,experimenter_id,active,role) values ('%s',%s,'True','%s')" % (exp, experimenter_id, role)
        logging.debug("    add_relationship: inserted experiment=%s  experimenter_id=%s (%s) role=%s", exp, experimenter_id, username, role)
        cursor.execute(sql)
    else:
        sql = "update experiments_experimenters set active=true, role='%s' where experiment='%s' and experimenter_id=%s" % (role, exp, experimenter_id)
        logging.debug("    add_relationship: updated experiment=%s  experimenter_id=%s (%s) role=%s active=true", exp, experimenter_id, username, role)
        cursor.execute(sql)

def set_active_status(cursor, active_status, exp):
    for username, user in active_status.items():
        sql = "update experiments_experimenters set active=%s where experiment='%s' and experimenter_id=%s" % (user['active'], exp, user['experimenter_id'])
        cursor.execute(sql)
        logging.debug("set_active_status: experiment=%s experimenter_id=%s (%s) active set to: %s", exp, user['experimenter_id'], username, user['active'])

def set_assigned_role(cursor, role_changes, exp):
    for username, user in role_changes.items():
        sql = "update experiments_experimenters set role='%s', active=true where experiment='%s' and experimenter_id=%s" % (user['role'], exp, user['experimenter_id'])
        cursor.execute(sql)
        logging.debug("set_assigned_role: experiment=%s experimenter_id=%s (%s) role=%s", exp, user['experimenter_id'], username, user['role'])

def set_logging(log_dir, verbose):
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)
    if log_dir and not os.path.exists(log_dir):
        os.system("mkdir -p %s" % log_dir)
    if log_dir:
        log_file = "%s/addusers-%s.log" % (log_dir, time.strftime('%Y-%m-%d-%H%M%S'))
        if not os.path.exists(log_file):
            os.system("touch %s" % log_file)
        fileHandler = logging.FileHandler(log_file)
        fileHandler.setFormatter(logFormatter)
        fileHandler.setLevel(logging.DEBUG)
        rootLogger.addHandler(fileHandler)
    if verbose:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        consoleHandler.setLevel(logging.DEBUG)
        rootLogger.addHandler(consoleHandler)

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

    exp_list = get_experiments(cursor, args.experiment)
    logging.debug("main:Experiment todo list: %s", exp_list)
    for exp in exp_list:
        logging.debug("main: Experiment: %s", exp)
        users = {}
        if args.ferry:
            users = get_ferry_data(args.cert, args.ferry, exp, args.skip_analysis)
        else:
            users = get_voms_data(args.cert, exp)
        new_users, active_status, role_changes = determine_changes(cursor, exp, users)
        add_users(cursor, exp, new_users)
        set_active_status(cursor, active_status, exp)
        set_assigned_role(cursor, role_changes, exp)

        if args.commit is True:
            conn.commit()
            logging.debug("main: %s database changes COMMITED!!", exp)
            add_to_listserv(args.list_owner, new_users)
        else:
            conn.rollback()
            logging.debug("main: %s database changes ROLLED BACK!!", exp)


    cursor.close()
    conn.close()
    # have poms clear its permissions cache since we updated permissions
    req = requests.get("http://127.0.0.1:8080/poms/clear_cache")
    logging.debug("main: elapsed seconds: %s", (time.time() - stime))
    logging.debug("main: finito!")

if __name__ == "__main__":
    main()
