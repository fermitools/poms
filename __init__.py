
import os
import sys

os.environ['POMS_DIR'] = __file__[:__file__.rfind('/')]
if "ENV" in os.environ:
    if os.environ["ENV"] == "dev":
        os.environ["WEB_CONFIG"] = "/home/poms/private/poms/config/dev.webservice.toml"
    elif os.environ["ENV"] == "prod":
        os.environ["WEB_CONFIG"] = "/home/poms/private/poms/config/prod.webservice.toml"
    else:
        os.environ["WEB_CONFIG"] = "/home/poms/private/poms/config/prod.webservice.toml"
elif "WEB_CONFIG" not in os.environ:
    os.environ["WEB_CONFIG"] = "/home/poms/private/poms/config/prod.webservice.toml"
#print "set POMS_DIR to " , os.environ['POMS_DIR']
