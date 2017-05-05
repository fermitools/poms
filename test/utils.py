import configparser
import getpass

import time
import subprocess
import sys
import os

# put envioronment vars into some config sections...

def get_config_py():

    class fakeconfig(configparser.SafeConfigParser):
        def newget(self, var, default = None):
            return self.oldget('global',var, default)

    fakeconfig.oldget = fakeconfig.get
    fakeconfig.get = fakeconfig.newget
    config = fakeconfig()
    config = get_config(config)
    return config
    
def get_config(config = None):
    from textwrap import dedent
    from io import StringIO

    if config == None:
        config = configparser.RawConfigParser()
    configfile = '../webservice/poms.ini'
    confs = dedent("""
       [/static]
       HOME="%(HOME)s"
       POMS_DIR="%(POMS_DIR)s"
       [global]
       HOME="%(HOME)s"
       POMS_DIR="%(POMS_DIR)s"
       [POMS]
       HOME="%(HOME)s"
       POMS_DIR="%(POMS_DIR)s"
    """ % os.environ)
    
    cf = open(configfile,"r")
    confs = confs + cf.read()
    cf.close()

    config.readfp(StringIO(confs))
    return config


def get_pid():
    config = get_config()

    pid_path = config.get('global', 'log.pidfile')[1:-1]
    with open(pid_path, 'r') as f:
        pid = f.readline()
    f.close()
    return pid


def get_db_info():
    config = get_config()

    dbname = config.get('global', 'db').replace("\"", "'")
    dbuser = config.get('global', 'dbuser').replace("\"", "'")
    dbhost = config.get('global', 'dbhost').replace("\"", "'")
    dbport = config.get('global', 'dbport').replace("\"", "'")
    try:
        dbpass = config.get('global', 'dbpass').replace("\"", "'")
    except configparser.NoOptionError as e:
        dbpass = getpass.getpass("Please enter database password: ")
    return dbhost, dbname, dbuser, dbpass, dbport



 


def get_base_url():
    config = get_config()

    port = config.get('global', 'server.socket_port')
    pomspath = config.get('global', 'pomspath')
    pomspath = pomspath.replace("'", "")
    # pidpath = config.get('global', 'log.pidfile')[1:-1]
    base_url = "http://localhost:" + port + pomspath + "/"
    return base_url


def setUpPoms():
    print("************* SETTING UP POMS *************")
    try:
        # proc = subprocess.Popen("cd ../ && source /fnal/ups/etc/setups.sh && setup -. poms && cd webservice/ && python service.py --no-wsgi",
        #                         shell=True)
        proc = subprocess.Popen("python ../webservice/service.py --no-wsgi -c ../webservice/poms.ini > webservice.out 2>&1", shell=True)
        print("PID =", proc.pid)
    except OSError as e:
        print("Execution failed:", e)
    time.sleep(5)
    return proc


def tearDownPoms(proc):
    print("************* TEARING DOWN POMS *************")
    print("PID =", proc.pid)
    proc.kill()
    # pid = get_pid(p)
    # try:
    #     proc = subprocess.Popen("kill " + pid, shell=True)
    # except OSError as e:
    #     print >>sys.stderr, "Excecution failed:", e
