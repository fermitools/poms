#!/bin/env python

import os
import argparse
import time
import requests
import psycopg2

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
    parser = argparse.ArgumentParser(description='Loads a VOMS generated JSON file of experimenters into POMS.')
    parser.add_argument('host', help="Host database is on.")
    parser.add_argument('port', type=int, help="Database Port number.")
    parser.add_argument('dbname', help="Database to connect to.")
    parser.add_argument('user', help="User to connect as")
    parser.add_argument('-p', '--password', help="Password for the database user account. For testing only, for production use .pgpass .")
    parser.add_argument('-d', '--debug', action="store_true", help="Send debug data to screen")
    parser.add_argument('-l', '--log', help="Directory/filename for logging debug data.")
    args = parser.parse_args()
    # Get password if not suppled
    #d = vars(args)
    #if d['password'] == None:
    #    d['password'] = getpass.getpass('Password: ')
    return args


def get_experiments(cursor):
    exp_list = []
    cursor.execute("select experiment from experiments where experiment != 'samdev'")
    for row in cursor.fetchall():
        exp_list.append(row[0])
    return exp_list

def get_voms_data(exp):
    debug("get_voms_data for: %s" % exp)
    payload = {"accountName": "%spro" % exp}
    r = requests.get("https://gums2.fnal.gov:8443/gums/map_account.jsp", params=payload, verify=False, cert=('/home/poms/private/gsi/pomscert.pem','/home/poms/private/gsi/pomskey.pem'))
    users = {}
    for line in r.iter_lines():
        if line.find("CN=UID:") != -1:
            username=line.split("/CN=")[-1][4:]
            commonname=line.split("/CN=")[-2]
            # build a dictionary to get rid of duplicates
            users[username]=commonname
    return users


def add_user(cursor,username,commonname,session_experiment):
    sql = "select experimenter_id from experimenters where username='%s'" % username
    cursor.execute(sql)
    (experimenterid,) = cursor.fetchone() or (None,)
    if experimenterid is None:
        # If the experimenter has only two names, then split into first/last.
        last_name = commonname
        first_name = ""
        names = commonname.split(" ")
        if len(names) == 2:
            (first_name, last_name) = commonname.split(" ")
        sql = "insert into experimenters (last_name,first_name,username,session_experiment) values ('%s','%s','%s','%s') returning experimenter_id" % (last_name,first_name,username,session_experiment)
        cursor.execute(sql)
        experimenterid = cursor.fetchone()[0]
        debug("add_user: inserted new experimenter id=%s" % experimenterid)
    return experimenterid

def add_relationship(cursor,experimenterid,exp):
    sql = "select 1 from experiments_experimenters where experiment = '%s' and experimenter_id = %s" % (exp,experimenterid)
    cursor.execute(sql)
    if cursor.fetchone() is None:
        sql = "insert into experiments_experimenters (experiment,experimenter_id,active) values ('%s',%s,'True')" % (exp,experimenterid)
        debug("add_relationship: inserted experiment=%s  experimenter_id=%s" % (exp,experimenterid))
        cursor.execute(sql)

def main():
    global dbg
    global log
    args = parse_command_line()
    if args.debug:
        dbg = True
    if args.log:
        logfile = "%s-%s.log" % (args.log,time.strftime('%Y%m%d%H%M%S'))
        log = open(logfile,'w')
    password=""
    if args.password:
        password="password=%s" % args.password
    conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s %s" % (args.dbname, args.host, args.port, args.user, password))
    cursor = conn.cursor()

    exp_list = get_experiments(cursor)
    debug("main:Experiments in experiment table: %s" % exp_list)

    for exp in exp_list:
        voms_data = get_voms_data(exp)
        debug("main: Experiment: %s" % exp)

        for username in voms_data.keys():
            commonname = voms_data[username]
            debug("main: user <%s> <%s>" % (username, commonname))
            experimenterid = add_user(cursor, username, commonname, exp)
            add_relationship(cursor, experimenterid, exp)
        conn.commit()

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
