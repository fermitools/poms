import sys
import os
import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class DBHandle:
    def __init__(self):
	cf = ConfigParser.SafeConfigParser()
	cf.read("../webservice/poms.ini")
	cf.read("../webservice/passwd.ini")
	db =cf.get("global","db").strip('"')
	dbuser = cf.get("global","dbuser").strip('"')
	dbpass = cf.get("global","dbpass").strip('"')
	dbhost = cf.get("global","dbhost").strip('"')
	dbport = cf.get("global","dbport").strip('"')
	db_path = "postgresql://%s:%s@%s:%s/%s" % (dbuser, dbpass, dbhost, dbport,db)
	sa_engine = create_engine(db_path, echo=False)
	Session = sessionmaker(bind=sa_engine)
	self.dbhandle = Session()

    def get(self):
	return self.dbhandle
