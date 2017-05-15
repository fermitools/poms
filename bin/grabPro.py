#!/bin/env python

import requests
import json

requests.packages.urllib3.disable_warnings()
data = {}
data["prousers"] = {}
experiments = ["lariat","nova","uboone"]
for exp in experiments:
    data["prousers"][exp] = []
    payload = {"accountName": "%spro" % exp}
    r = requests.get("https://gums2.fnal.gov:8443/gums/map_account.jsp", params=payload, verify=False, cert=('/home/poms/private/gsi/pomscert.pem','/home/poms/private/gsi/pomskey.pem'))
    users = {}
    for line in r.iter_lines():
        if line.find("CN=UID:") != -1:
            username=line.split("/CN=")[-1][4:]
            commonname=line.split("/CN=")[-2]
            # build a dictionary to get rid of duplicates
            users[username]=commonname

    for user in users:
        userdict = {}
        userdict["username"] = user
        userdict["commonname"] = users[user]
        data["prousers"][exp].append(userdict)

print(json.dumps(data))

