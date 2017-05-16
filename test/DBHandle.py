import sys
import os
import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import poms

class fake_cherrypy_config:
    def __init__(self, cf):
        self.cf = cf
    def get(self, tag, default = None):
        return self.cf.get('global',tag)

class DBHandle:
    def __init__(self):
    '''
    ###HEAD
	cf = ConfigParser.SafeConfigParser()
	cf.read("%s/webservice/poms.ini" % os.environ['POMS_DIR'])
	db =cf.get("Databases","db").strip('"')
	dbuser = cf.get("Databases","dbuser").strip('"')
	dbhost = cf.get("Databases","dbhost").strip('"')
	dbport = cf.get("Databases","dbport").strip('"')
    =======
    '''
        cf = configparser.SafeConfigParser()
        cf.read("%s/webservice/poms.ini" % os.environ['POMS_DIR'])
        db =cf.get("Databases","db").strip('"')
        dbuser = cf.get("Databases","dbuser").strip('"')
        dbhost = cf.get("Databases","dbhost").strip('"')
        dbport = cf.get("Databases","dbport").strip('"')
     #develop
        # Do NOT use a password here; use .pgpass as we do in production!
        db_path = "postgresql://%s:@%s:%s/%s" % (dbuser, dbhost, dbport, db)
        sa_engine = create_engine(db_path, echo=False)
        Session = sessionmaker(bind=sa_engine)
        self.dbhandle = Session()
        self.cf = fake_cherrypy_config(cf)

    def get(self):
        return self.dbhandle
