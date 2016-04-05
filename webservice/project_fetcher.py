#!/usr/bin/env python

import urllib2
import urllib
import json
import time
import concurrent.futures
import requests
#~ import pprint       #VP Debug

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

        if not experiment or not projid:
            return {}

        if self.have_cache(experiment, projid):
            return self.proj_cache[experiment+projid]

        base = "http://samweb.fnal.gov:8480"
        url = "%s/sam/%s/api/projects/name/%s/summary?format=json" % (base, experiment, projid)
        try:
            res = urllib2.urlopen(url)
            print "url=%s" % url
            text = res.read()
            info = json.loads(text)
            res.close()
            self.do_totals(info)
            self.proj_cache[experiment+projid] = info
            self.proj_cache_time[experiment+projid] = time.time()
            return info
        except:
            return {}

    def fetch_info_list(self, task_list):
        """
        """
        #~ return [ {"tot_consumed": 0, "tot_failed": 0, "tot_jobs": 0, "tot_jobfails": 0} ] * len(task_list)    #VP Debug
        base = "http://samweb.fnal.gov:8480"
        urls = ["%s/sam/%s/api/projects/name/%s/summary?format=json" % (base, t.campaign_obj.experiment, t.project) for t in task_list]
        #~ print "urls=",          #VP Debug
        #~ pprint.pprint(urls)     #VP Debug
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            replies = executor.map(requests.get, urls)
        infos = []
        for r in replies:
            try:
                info = json.loads(r.text)
                self.do_totals(info)
                infos.append(info)
            except:
                infos.append({})
        #~ pprint.pprint(infos[:3])   #VP Debug
        return infos

    def do_totals(self,info):
        tot_consumed = 0
        tot_failed = 0
        tot_jobs = 0
        tot_jobfails = 0
        for proc in info["processes"]:
             tot_consumed += proc["counts"]["consumed"]
             tot_failed += proc["counts"]["failed"]
             tot_jobs += 1
             if proc["status"] != "completed":
                 tot_jobfails += 1

        info["tot_consumed"] = tot_consumed
        info["tot_failed"] = tot_failed
        info["tot_jobs"] = tot_jobs
        info["tot_jobfails"] = tot_jobfails

    def list_files(self, experiment, dims):
        base = "http://samweb.fnal.gov:8480"
        url="%s/sam/%s/api/files/list" % (base,experiment)
        try:
            res = urllib2.urlopen(url,urllib.urlencode({"dims":dims,"format":"json"}))
            text = res.read()
            fl = json.loads(text)
            res.close()
        except:
            raise
            fl = []
        return fl

    def create_definition(self, experiment, name, dims):
        base = "http://samweb.fnal.gov:8480"
        url = "%s/sam/%s/api/definitions/create" % (base, experiment)
        try:
            res = urllib2.urlopen(url,urllib.urlencode({"name": name, "dims":dims,"user":os.environ.get("USER",os.environ.get("GRID_USER","sam"))}))
            text = res.read()
            res.close()
        except:
            raise
        return "Ok."

if __name__ == "__main__":
    import pprint
    pf = project_fetcher()
    i = pf.fetch_info("nova","brebel-AnalysisSkimmer-20151120_0126")
    i2 = pf.fetch_info("nova","brebel-AnalysisSkimmer-20151120_0126")
    print "got:"
    pprint.pprint(i)
    print "got:"
    pprint.pprint(i2)

    l = pf.list_files("nova","file_name neardet_r00011388_s00_t00_S15-12-07_v1_data_keepup.caf.root,"
                        "neardet_r00011388_s00_t00_S15-12-07_v1_data_keepup.reco.root,"
                        "neardet_r00011388_s05_t00_S15-12-07_v1_data_keepup.reco.root,"
                        "neardet_r00011388_s05_t00_S15-12-07_v1_data_keepup.caf.root,"
                        "neardet_r00011388_s01_t00_S15-12-07_v1_data_keepup.reco.root,"
                        "neardet_r00011388_s01_t00_S15-12-07_v1_data_keepup.caf.root,"
                        "neardet_r00011388_s10_t00_S15-12-07_v1_data_keepup.reco.root")
    print("got list:")
    pprint.pprint(l)
