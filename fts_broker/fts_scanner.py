#!/usr/bin/env python
import urllib2
import urllib
import httplib
import json
import shelve
import os
import time

class fts_status_watcher:
    def __init__(self, debug=0):
        self.registry_url = 'http://samweb.fnal.gov:8480/sam_web_registry/?format=json'
        self.debug = debug
        self.workdir = "/home/poms/private/var/ftsscanner"

    def fetch_json(self, url):
        if self.debug: print "fetching: " , url
        res = None
        try:
	    res = urllib2.urlopen(url)
	    text = res.read()
	    info = json.loads(text)
	    res.close()
        except:
            if res: res.close()
            return {}

        return info
    
    def scan(self):
        fs = None
        didthat = {"none":True}
        explist = {}
        oldexp = None
        registry = self.fetch_json(self.registry_url)
        print "got registry:", registry
        for item in registry:
            if item["type"] == "fts" and item["tier"] == "prd":
               # some fts-es are listed twice...
               if didthat.get(item["uris"].get("service","none"), False):
                   continue
               # skip daq fts-es
               if item["uris"]["service"].find("daq") > 0:
                   continue
               if item["experiment"] == "unknown":
                   continue

               explist[item["experiment"]] = True
               didthat[item["uris"]["service"]] = True

               if oldexp != item["experiment"]:
                   if fs: fs.close()
                   fs = shelve.open("%s/%s_files.db.new" % (self.workdir, item["experiment"]),flag="c")
               status = self.fetch_json(item["uris"]["service"] + "/status?format=json")
               print "staus.keys() is: " , status.keys()
               for t in status.get("errorstates",[]) + status.get("pendingstates",[]) + status.get("newstates",[]):
                   k = t["name"].encode('ascii','ignore')
                   v = t["msg"].encode('ascii','ignore')
                   fs[k] = "%s:%s" % (item["name"], v)


        if fs: fs.close()

        for exp in explist.keys():
            if exp == "unknown":
                continue
            try:
                os.rename("%s/%s_files.db.new" % (self.workdir, item["experiment"]),
                         "%s/%s_files.db" % (self.workdir, item["experiment"]))
            except:
                print "Error renaming %s/%s_files.db.new" % (self.workdir, item["experiment"])
 
    def poll(self):
        while 1:
            self.scan()
            # emprically we run in about 2 minutes, so this is
            # about a 50% duty cycle.
            time.sleep(120)

if __name__ == "__main__":
    w = fts_status_watcher()
    w.poll()
