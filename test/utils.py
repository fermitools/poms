import ConfigParser
import getpass

import time
import subprocess
import sys


def get_pid():
    config = ConfigParser.RawConfigParser()
    config.read('../webservice/poms.ini')

    pid_path = config.get('global', 'log.pidfile')[1:-1]
    with open(pid_path, 'r') as f:
        pid = f.readline()
    f.close()
    return pid


def get_db_info():
    config = ConfigParser.ConfigParser()
    config.read('../webservice/poms.ini')

    dbname = config.get('global', 'db').replace("\"", "'")
    dbuser = config.get('global', 'dbuser').replace("\"", "'")
    dbhost = config.get('global', 'dbhost').replace("\"", "'")
    dbport = config.get('global', 'dbport').replace("\"", "'")
    try:
        dbpass = config.get('global', 'dbpass').replace("\"", "'")
    except ConfigParser.NoOptionError as e:
        dbpass = getpass.getpass("Please enter database password: ")
    return dbhost, dbname, dbuser, dbpass, dbport


def get_base_url():
    config = ConfigParser.RawConfigParser()
    config.read('../webservice/poms.ini')

    port = config.get('global', 'server.socket_port')
    pomspath = config.get('global', 'pomspath')
    pomspath = pomspath.replace("'", "")
    # pidpath = config.get('global', 'log.pidfile')[1:-1]
    base_url = "http://localhost:" + port + pomspath + "/"
    return base_url


def setUpPoms():
    print "************* SETTING UP POMS *************"
    try:
        # proc = subprocess.Popen("cd ../ && source /fnal/ups/etc/setups.sh && setup -. poms && cd webservice/ && python service.py &", shell=True)
        proc = subprocess.Popen("/home/podstvkv/Workspace/Poms/run-uwsgi-test.sh",
                                cwd='/home/podstvkv/Workspace/Poms',
                                shell=False)
        print "PID =", proc.pid
    except OSError as e:
        print "Execution failed:", e
    time.sleep(5)
    return proc


def tearDownPoms(proc):
    print "************* TEARING DOWN POMS *************"
    print "PID =", proc.pid
    proc.kill()
    # pid = get_pid(p)
    # try:
    #     proc = subprocess.Popen("kill " + pid, shell=True)
    # except OSError as e:
    #     print >>sys.stderr, "Excecution failed:", e
