#!/bin/env python

import sys
import argparse
import time
import psycopg2
import requests

if sys.version_info > (3, 0):
    import urllib3
else:
    import requests.packages.urllib3 as urllib3
urllib3.disable_warnings()

dbg = False
log = None

def debug(message):
    global dbg
    global log
    if dbg:
        print(message)
    if log:
        log.write(message+'\n')

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
    parser.add_argument('-d', '--debug', action="store_true", help="Send debug data to screen")
    parser.add_argument('-l', '--log', help="Directory/filename for logging debug data.")
    args = parser.parse_args()
    # Get password if not suppled
    #d = vars(args)
    #if d['password'] == None:
    #    d['password'] = getpass.getpass('Password: ')

    return args


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
    results = {}
    debug("query_ferry for experiment: %s  role: %s" % (exp, role))
    url = ferry_url + "/getAffiliationMembersRoles?experimentname=%s&rolename=/%s/Role=%s" % (exp, exp, role)
    debug("query_ferry: requested url: %s" % url)
    r = requests.get(url, verify=False, cert=('%s/pomscert.pem' % cert, '%s/pomskey.pem' % cert))
    if r.status_code != requests.codes.ok:
        debug("get_ferry_experiment_users -- error status_code: %s  -- %s" % (r.status_code, url))
    else:
        results = r.json()
        ferry_error = results.get('ferry_error', None)
        if ferry_error is not None:
            debug("get_ferry_experiment_users -- ferry_error: %s  URL: %s" % (str(ferry_error), url))
            results = {}
    return results.get(exp, {})

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
    if skip_analysis is False:
        anal_users = query_ferry(cert, ferry_url, exp, 'Analysis')
    prod_users = query_ferry(cert, ferry_url, exp, 'Production')

    for anal in anal_users:
        users[anal.get('username')] = {'commonname': anal.get('commonname'), 'role': 'analysis'}
    for prod in prod_users:
        users[prod.get('username')] = {'commonname': prod.get('commonname'), 'role': 'production'}
    return users

def get_voms_data(cert, exp):
    debug("get_voms_data for: %s" % exp)
    payload = {"accountName": "%spro" % exp}
    req = requests.get("https://gums2.fnal.gov:8443/gums/map_account.jsp", params=payload, verify=False, cert=('%s/pomscert.pem' % cert, '%s/pomskey.pem' % cert))
    users = {}
    for line in req.iter_lines():
        line = line.decode('utf-8')
        if line.find("CN=UID:") != -1:
            username = line.split("/CN=")[-1][4:]
            commonname = line.split("/CN=")[-2]
            # build a dictionary to get rid of duplicates, make it the
            # same structure as get_ferry_data builds
            users[username] = {'commonname': commonname, 'role': 'production'}
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

    cursor.execute(sql)
    for (username, experimenter_id, active, role,) in cursor.fetchall():
        user = new_users.pop(username, None)
        if user is None:
            if active is True:
                active_status[username] = {'experimenter_id' : experimenter_id, 'active': False, 'username': username}
        elif user['role'] != role:
            if role != 'coordinator':
                role_changes[username] = {'experimenter_id': experimenter_id, 'role': role}
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
            sql = """
                insert into experimenters (last_name,first_name,username,session_experiment)
                    values ('%s','%s','%s','%s') returning experimenter_id
            """ % (last_name, first_name, username, exp)
            cursor.execute(sql)
            experimenter_id = cursor.fetchone()[0]
            debug("add_user: inserted new experimenter id=%s username: %s commonname: %s " % (experimenter_id, username, user['commonname']))
        add_relationship(cursor, experimenter_id, exp, user['role'], username)
    return

def add_relationship(cursor, experimenter_id, exp, role, username):
    sql = "select 1 from experiments_experimenters where experiment = '%s' and experimenter_id = %s" % (exp, experimenter_id)
    cursor.execute(sql)
    if cursor.fetchone() is None:
        sql = "insert into experiments_experimenters (experiment,experimenter_id,active,role) values ('%s',%s,'True','%s')" % (exp, experimenter_id, role)
        debug("    add_relationship: inserted experiment=%s  experimenter_id=%s (%s) role=%s" % (exp, experimenter_id, username, role))
        cursor.execute(sql)
    else:
        sql = "update experiments_experimenters set active=true, role='%s' where experiment='%s' and experimenter_id=%s" % (role, exp, experimenter_id)
        debug("    add_relationship: updated experiment=%s  experimenter_id=%s (%s) role=%s active=true" % (exp, experimenter_id, username, role))
        cursor.execute(sql)

def set_active_status(cursor, active_status, exp):
    for username, user in active_status.items():
        sql = "update experiments_experimenters set active=%s where experiment='%s' and experimenter_id=%s" % (user['active'], exp, user['experimenter_id'])
        cursor.execute(sql)
        debug("set_active_status: experiment=%s experimenter_id=%s (%s) active set to: %s" % (exp, user['experimenter_id'], username, user['active']))

def set_assigned_role(cursor, role_changes, exp):
    for username, user in role_changes.items():
        sql = "update experiments_experimenters set role='%s' where experiment='%s' and experimenter_id=%s active=true" % (user['role'], exp, user['experimenter_id'])
        cursor.execute(sql)
        debug("set_assigned_role: experiment=%s experimenter_id=%s (%s) role=%s" % (exp, user['experimenter_id'], username, user['role']))

def main():
    global dbg
    global log
    stime = time.time()
    args = parse_command_line()

    if args.debug:
        dbg = True

    if args.log:
        logfile = "%s-%s.log" % (args.log, time.strftime('%Y%m%d%H%M%S'))
        log = open(logfile, 'w')
        sys.stderr = log

    password = ""
    if args.password:
        password = "password=%s" % args.password

    debug("main:POMS Database: %s Host: %s Port:%s" % (args.dbname, args.host, args.port))
    conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s %s" % (args.dbname, args.host, args.port, args.user, password))
    cursor = conn.cursor()

    exp_list = get_experiments(cursor, args.experiment)
    debug("main:Experiment todo list: %s" % exp_list)
    for exp in exp_list:
        debug("\nmain: Experiment: %s" % exp)
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
            debug("\nmain: %s database changes COMMITED!!" % exp)
        else:
            conn.rollback()
            debug("\nmain: %s database changes ROLLED BACK!!" % exp)


    cursor.close()
    conn.close()
    debug("main: elapsed seconds: %s" % (time.time() - stime))
    debug("main: finito!")

if __name__ == "__main__":
    main()
