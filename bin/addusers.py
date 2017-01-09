#!/bin/env python

import os.path, argparse, psycopg2, json

dbg = False

def debug(message):
    global dbg
    if dbg:
        print message

def parse_command_line():
    parser = argparse.ArgumentParser(description='Loads a VOMS generated JSON file of experimenters into POMS.')
    parser.add_argument('host', help="Host database is on.")
    parser.add_argument('dbname', help="Database to connect to.")
    parser.add_argument('port', type=int, help="Database Port number.")
    parser.add_argument('file', help="File of users to read in.")
    #parser.add_argument('-p', '--password', help="Password for the database user account.")
    parser.add_argument('-d', '--debug', action="store_true", help="Debug")
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

def add_user(cursor,username,commonname):
    username = username + "@fnal.gov" 
    sql = "select experimenter_id from experimenters where email='%s'" % username
    cursor.execute(sql)
    (experimenterid,) = cursor.fetchone() or (None,)
    if experimenterid is None:
        sql = "insert into experimenters (last_name,email) values ('%s','%s') returning experimenter_id" % (commonname,username)
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
    args = parse_command_line()
    if args.debug:
        dbg = True
    if os.path.isfile(args.file) == False:
        print "missing file: %s" % args.file
        raise SystemExit 
    else:
        json_data = open(args.file)
        adict = json.load(json_data)
        debug("main:json parsed file %s"% args.file)
        expdict = adict['prousers']
        conn = psycopg2.connect("dbname=%s host=%s port=%s" % (args.dbname, args.host, args.port))
        cursor = conn.cursor()
        for exp in expdict.keys():
            if verify_exp(cursor,exp) == False:
                print 'experiment: %s does not exist in poms' % exp
            else:
                for user in expdict[exp]:
                    debug("main: processing <%s> <%s> <%s>" % (exp, user['username'], user['commonname'])) 
                    experimenterid = add_user(cursor, user['username'], user['commonname'])
                    add_relationship(cursor, experimenterid, exp)
        conn.commit()
        conn.close()


if __name__ == "__main__":
    main()

