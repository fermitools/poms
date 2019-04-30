import configparser
import getpass

import time
import subprocess
import sys
import os
import poms

from poms.webservice.logit import setlevel, log, DEBUG, INFO, CRITICAL
import logging.config
from poms.webservice import logging_conf


def beverbose():
    logging.config.dictConfig(logging_conf.LOG_CONF)
    setlevel(level=DEBUG)
    log(CRITICAL, "testing 1 2 3")
    log(INFO, "testing 4 5 6")
    log(DEBUG, "testing 7 8 9")


# put envioronment vars into some config sections...


class fakeconfig:
    def __init__(self, *args, **kwargs):
        self.cf = configparser.SafeConfigParser(*args, **kwargs)

    def read_file(self, file):
        return self.cf.read_file(file)

    def write(self, file):
        return self.cf.write(file)

    def get(self, var, default=None):
        try:
            res = self.cf.get("global", var)
            if res[0] == "'":
                res = res[1:-1]
            return res
        except:
            return default


def get_config_py():

    config = get_config()
    return config


def get_config(config=None):
    from textwrap import dedent
    from io import StringIO

    if config == None:
        config = fakeconfig()

    configfile = "../webservice/poms.ini"
    confs = dedent(
        """
       [DEFAULT]
       HOME="%(HOME)s"
       POMS_DIR="%(POMS_DIR)s"
    """
        % os.environ
    )

    cf = open(configfile, "r")
    confs = confs + cf.read()
    cf.close()

    config.read_file(StringIO(confs))

    print("in get_config, returning:")
    config.write(sys.stdout)
    return config


def get_pid():
    config = get_config()

    pid_path = config.get("log.pidfile")[1:-1]
    with open(pid_path, "r") as f:
        pid = f.readline()
    f.close()
    return pid


def get_db_info():
    config = get_config()

    dbname = config.get("db").replace('"', "'")
    dbuser = config.get("dbuser").replace('"', "'")
    dbhost = config.get("dbhost").replace('"', "'")
    dbport = config.get("dbport").replace('"', "'")
    try:
        dbpass = config.get("dbpass").replace('"', "'")
    except configparser.NoOptionError as e:
        dbpass = getpass.getpass("Please enter database password: ")
    return dbhost, dbname, dbuser, dbpass, dbport


def get_base_url():
    config = get_config()

    port = config.get("server.socket_port")
    pomspath = config.get("pomspath")
    pomspath = pomspath.replace("'", "")
    # pidpath = config.get('log.pidfile')[1:-1]
    base_url = "http://localhost:" + port + pomspath + "/"
    return base_url


def setUpPoms():
    print("************* SETTING UP POMS *************")
    try:
        f = open("webservice.out", "w")
        proc = subprocess.Popen(
            ["python", "../webservice/service.py", "--no-wsgi", "-cs", "../webservice/poms.ini"], stdout=f, stderr=f
        )
        print("PID =", proc.pid)
    except OSError as e:
        print("Execution failed:", e)
    time.sleep(5)
    return proc


def tearDownPoms(proc):
    print("************* TEARING DOWN POMS *************")
    print("PID =", proc.pid)
    for i in range(5):
        try:
            proc.kill()
            time.sleep(1)
        except:
            break

    # pid = get_pid(p)
    # try:
    #     proc = subprocess.Popen("kill " + pid, shell=True)
    # except OSError as e:
    #     print >>sys.stderr, "Excecution failed:", e


def setup_ifdhc():
    import sys
    import os

    sys.path.insert(0, os.environ["SETUPS_DIR"])
    import setups

    ups = setups.setups()
    ups.setup("ifdhc")
