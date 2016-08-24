#!/usr/bin/env python

#import urllib2
#import httplib
import urllib
import json
import time
import concurrent.futures
import requests
import traceback
import os
import cherrypy
import ssl

class samweb_lite:
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
            res = requests.get(url)
            info = res.json()
            res.close()
            self.do_totals(info)
            self.proj_cache[experiment+projid] = info
            self.proj_cache_time[experiment+projid] = time.time()
            return info
        except:
            traceback.print_exc()
            return {}

    def fetch_info_list(self, task_list):
        """
        """
        #~ return [ {"tot_consumed": 0, "tot_unknown": 0, "tot_jobs": 0, "tot_jobfails": 0} ] * len(task_list)    #VP Debug
        base = "http://samweb.fnal.gov:8480"
        urls = ["%s/sam/%s/api/projects/name/%s/summary?format=json" % (base, t.campaign_obj.experiment, t.project) for t in task_list]
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            replies = executor.map(requests.get, urls)
        infos = []
        for r in replies:
            try:
                info = r.json()
                self.do_totals(info)
                infos.append(info)
            except:
                infos.append({})
        return infos

    def do_totals(self,info):
        tot_consumed = 0
        tot_skipped = 0
        tot_failed = 0
        tot_jobs = 0
        tot_jobfails = 0
        for proc in info["processes"]:
             tot_consumed += proc["counts"]["consumed"]
             tot_failed += proc["counts"]["failed"]
             tot_skipped += proc["counts"].get("skipped", 0)
             tot_jobs += 1
             if proc["status"] != "completed":
                 tot_jobfails += 1

        info["tot_consumed"] = tot_consumed
        info["tot_failed"] = tot_failed
        info["tot_skipped"] = tot_skipped
        info["tot_jobs"] = tot_jobs
        info["tot_jobfails"] = tot_jobfails

    def list_files(self, experiment, dims):
        base = "http://samweb.fnal.gov:8480"
        url="%s/sam/%s/api/files/list" % (base,experiment)
        try:
            res = requests.get(url,params={"dims":dims,"format":"json"})
            fl = res.json()
            res.close()
        except:
            raise
            fl = []
        return fl

    def count_files(self,experiment,dims):
        base = "http://samweb.fnal.gov:8480"
        url="%s/sam/%s/api/files/count" % (base,experiment)
        try:
            res = requests.get(url,params={"dims":dims})
            text = res.content
            count = int(text)
            res.close()
        except:
            raise
            count = 0
        return count

    def count_files_list(self, experiment, dims_list ):
        """
        """
        def getit(url):
            retries = 5
            r =  requests.get(url,verify=False)
            while r.status_code >= 500 and retries > 0:
               time.sleep(5)
               retries = retries - 1
               r =  requests.get(url,verify=False)
            return r
               
        base = "http://samweb.fnal.gov:8480"
        urls = ["%s/sam/%s/api/files/count?%s" % (base, experiment, urllib.urlencode({"dims":dims})) for dims in dims_list]
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            replies = executor.map(getit, urls)
        infos = []
        for r in replies:
            if r.text.find("query limit") > 0:
                infos.append(-1)
            else:     
                try:
                   infos.append(int(r.text)) 
                except:
                   infos.append(-1) 
        return infos

    def create_definition(self, experiment, name, dims):
        cherrypy.log("create_definition( %s, %s, %s )" % (experiment,name,dims))
        base = "https://samweb.fnal.gov:8483"
        path = "/sam/%s/api/definitions/create" %  experiment
        url = "%s%s" % (base,path)

        pdict = None
        try:
  
            pdict = {"defname": name, "dims":dims,"user":"sam", "group": experiment}
            cherrypy.log("create_definition: calling: %s with %s " % (url,pdict))
            res = requests.post(url,data=pdict,verify=False,cert=("%s/private/gsi/%scert.pem" % (os.environ["HOME"],os.environ["USER"]),"%s/private/gsi/%skey.pem" % (os.environ["HOME"],os.environ["USER"])))
 
            text = res.content
            cherrypy.log("definitions/create reuturns: %s" % text)
            res.close()
        except Exception as e:
            cherrypy.log( "Exception creating definition: url %s args %s exception %s" % ( url, pdict, e.args))
            return "Fail."
        return text

if __name__ == "__main__":
    import pprint
    sl = samweb_lite()
    print sl.create_definition("samdev","mwm_test_%d" % os.getpid(), "(snapshot_for_project_name mwm_test_proj_1465918505)")
    i = sl.fetch_info("nova","arrieta1-Offsite_test_Caltech-20160404_1157")
    i2 = sl.fetch_info("nova","brebel-AnalysisSkimmer-20151120_0126")
    print "got:"
    pprint.pprint(i)
    print "got:"
    pprint.pprint(i2)

    l = sl.list_files("nova","file_name neardet_r00011388_s00_t00_S15-12-07_v1_data_keepup.caf.root,"
                        "neardet_r00011388_s00_t00_S15-12-07_v1_data_keepup.reco.root,"
                        "neardet_r00011388_s05_t00_S15-12-07_v1_data_keepup.reco.root,"
                        "neardet_r00011388_s05_t00_S15-12-07_v1_data_keepup.caf.root,"
                        "neardet_r00011388_s01_t00_S15-12-07_v1_data_keepup.reco.root,"
                        "neardet_r00011388_s01_t00_S15-12-07_v1_data_keepup.caf.root,"
                        "neardet_r00011388_s10_t00_S15-12-07_v1_data_keepup.reco.root")
    print("got list:")
    pprint.pprint(l)

    c = sl.count_files("nova", "project_name 'vito-vito-calib-manual-Offsite-R16-01-27-prod2calib.e-neardet-20160210_1624','vito-vito-calib-manual-Offsite-R16-01-27-prod2calib.a-fardet-20160202_1814'");

    print "got count:", c

    l = sl.count_files_list("nova",["defname:mwm_test_6","defname:mwm_test_9","defname:mwm_test_11"])
    print "got count list: ", l

