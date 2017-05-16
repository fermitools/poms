#!/bin/env python

import os, argparse, psycopg2, json, time

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
    parser.add_argument('file', help="File of users to read in.")
    parser.add_argument('-p', '--password', help="Password for the database user account. For testing only, for production use .pgpass .")
    parser.add_argument('-d', '--debug', action="store_true", help="Send debug data to screen")
    parser.add_argument('-l', '--log', help="Directory/filename for logging debug data.")
    args = parser.parse_args()
    # Get password if not suppled
    #d = vars(args)
    #if d['password'] == None:
    #    d['password'] = getpass.getpass('Password: ')
    return args


def verify_exp(cursor,exp):
    retval = True
    sql = "select 1 from experiments where experiment='%s'" % exp
    cursor.execute(sql)
    row = cursor.fetchone()
    if row is None:
        retval = False
    return retval

def add_user(cursor,username,commonname,exp):
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
        sql = "insert into experimenters (last_name,first_name,username,session_experiment) values ('%s','%s','%s','%s') returning experimenter_id" % (last_name,first_name,username,exp)
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
    if os.path.isfile(args.file) == False:
        debug("main: missing file: %s" % args.file)
        print("addusers.py -- main: missing file: %s" % args.file)
        raise SystemExit
    else:
        json_data = open(args.file)
        adict = json.load(json_data)
        debug("main:json parsed file %s"% args.file)
        expdict = adict['prousers']
        password=""
        if args.password:
            password="password=%s" % args.password
        conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s %s" % (args.dbname, args.host, args.port, args.user, password))
        cursor = conn.cursor()
        for exp in list(expdict.keys()):
            if verify_exp(cursor,exp) == False:
                debug('experiment: %s does not exist in poms' % exp)
            else:
                for user in expdict[exp]:
                    debug("main: processing <%s> <%s> <%s>" % (exp, user['username'], user['commonname']))
                    experimenterid = add_user(cursor, user['username'], user['commonname'],exp)
                    add_relationship(cursor, experimenterid, exp)
        conn.commit()
        conn.close()
        json_data.close()
        os.rename(args.file,args.file+".loaded")


if __name__ == "__main__":
    main()
