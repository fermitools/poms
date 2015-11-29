#!/usr/bin/env python

import urllib2
import json
import time

class project_fetcher:
     def __init__(self):
         self.proj_cache = {}
         self.proj_cache_time = {}
         self.valid = 60

     def have_cache(self, experiment, projid):
         t = self.proj_cache_time.get(experiment+projid, 0)
         p = self.proj_cache.get(experiment+projid, None)

         if p and (time.time() - t < self.valid or 
                    p['project_status'] == "completed"):
              return 1

         return 0

     def fetch_info(self, experiment, projid):

         if self.have_cache(experiment,projid):
             return self.proj_cache[experiment+projid]

         base = "http://samweb.fnal.gov:8480"
         url="%s/sam/%s/api/projects/name/%s/summary?format=json" % (base,experiment, projid)
         res = urllib2.urlopen(url)
         text = res.read()
         info = json.loads(text)
         res.close()
         self.do_totals(info)
         self.proj_cache[experiment+projid] = info
         self.proj_cache_time[experiment+projid] = time.time()
         return info

     def do_totals(self, info):
         tot_consumed = 0
         tot_failed = 0
         for proc in info["processes"]:
              tot_consumed = tot_consumed + proc["counts"]["consumed"]
              tot_failed = tot_failed + proc["counts"]["failed"]

         info["tot_consumed"] = tot_consumed
         info["tot_failed"] = tot_failed


if __name__ == "__main__":
    pf = project_fetcher()
    i = pf.fetch_info("nova","brebel-AnalysisSkimmer-20151120_0126")
    i2 = pf.fetch_info("nova","brebel-AnalysisSkimmer-20151120_0126")
    print "got:", i
    print "got:", i2
