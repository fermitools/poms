#!/usr/bin/env python

import urllib
import time
import datetime
import concurrent.futures
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import traceback
import os
import cherrypy
from utc import utc
from poms.model.poms_model import FaultyRequest


def safe_get(sess, url, *args, **kwargs):
    """
    """
    #TODO: Need more refactoring to optimize
    reply = None
    dbh = kwargs.pop('dbhandle', None)

    if dbh is None:
        try:
            sess.mount('http://', HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.2)))
            reply = sess.get(url, timeout=5.0, *args, **kwargs)    # Timeout may need adjustment!
            if reply.status_code != 200:
                return None     # No need to return the response
        except:
            # Severe errors like network or DNS problems.
            traceback.print_exc()
            return None         # No need to return the response
        finally:
            if reply:
                reply.close()
        # Everything went OK
        return reply

    last_fault = dbh.query(FaultyRequest).filter(FaultyRequest.url == url).order_by(FaultyRequest.last_seen.desc()).first()
    # print "******* last_fault: {}".format(last_fault)
    if last_fault is not None:
        # Do some analysis for previous errors
        last_seen = last_fault.last_seen
        dt = (datetime.datetime.now(utc) - last_seen).total_seconds()
        if dt < 600.0:
            # Less than 10 minutes ago, let's skip it for now
            return None
        # It happened more than 10 minutes ago, let's try it again...
    try:
        sess.mount('http://', HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.2)))
        reply = sess.get(url, timeout=5.0, *args, **kwargs)    # Timeout may need adjustment!
        if reply.status_code != 200 and reply.status_code != 404:
            # Process error, store faulty query in DB
            fault = FaultyRequest(url=url, status=reply.status_code, message=reply.reason)
            dbh.add(fault)
            dbh.commit()
            return None     # No need to return the response
    except:
        # Process error!
        # Severe errors like network or DNS problems.
        traceback.print_exc()
        # Do the same?
        fault = FaultyRequest(url=reply.url, status=reply.status_code, message=reply.reason)
        dbh.add(fault)
        dbh.commit()
        return None         # No need to return the response
    finally:
        if reply:
            reply.close()
    # Everything went OK
    return reply


class samweb_lite:
    def __init__(self):
        self.proj_cache = {}
        self.proj_cache_time = {}
        self.valid = 60

    def flush(self):
        self.proj_cache = None
        self.proj_cache_time = None
        self.valid = 0

    def have_cache(self, experiment, projid):
        t = self.proj_cache_time.get(experiment + projid, 0)
        p = self.proj_cache.get(experiment + projid, None)

        if p and (time.time() - t < self.valid or p['project_status'] == "completed"):
            return 1

        return 0

    def fetch_info(self, experiment, projid, dbhandle=None):
        """
        """
        if not experiment or not projid or projid == "None":
            return {}

        if self.have_cache(experiment, projid):
            return self.proj_cache[experiment + projid]

        base = "http://samweb.fnal.gov:8480"
        url = "%s/sam/%s/api/projects/name/%s/summary?format=json" % (base, experiment, projid)
        with requests.Session() as sess:
            res = safe_get(sess, url, dbhandle=dbhandle)
        info = {}
        if res:
            info = res.json()
            self.do_totals(info)
            self.proj_cache[experiment + projid] = info
            self.proj_cache_time[experiment + projid] = time.time()
        return info


    def fetch_info_list(self, task_list, dbhandle=None):
        """
        """
        #~ return [ {"tot_consumed": 0, "tot_unknown": 0, "tot_jobs": 0, "tot_jobfails": 0} ] * len(task_list)    #VP Debug
        base = "http://samweb.fnal.gov:8480"
        urls = ["%s/sam/%s/api/projects/name/%s/summary?format=json" % (base, t.campaign_snap_obj.experiment, t.project) for t in task_list]
        with requests.Session() as sess:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                # replies = executor.map(sess.get, urls)
                replies = executor.map(lambda url: safe_get(sess, url, dbhandle=dbhandle), urls)
        infos = []
        for r in replies:
            if r:
                try:
                    info = r.json()
                    self.do_totals(info)
                    infos.append(info)
                except:
                    # Error in JSON parsing
                    traceback.print_exc()
                    infos.append({})
            else:
                infos.append({})
        return infos

    def do_totals(self, info):
        if not info.get("processes",None):
             return
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
        # we don't need the individual process info, just the totals..
        del info["processes"]

    def update_project_description(self, experiment, projname, desc):
        base = "http://samweb.fnal.gov:8480"
        url = "%s/sam/%s/api/projects/%s/%s/description" % (base, experiment, experiment, projname)
        r1 = None
        try:
            res = requests.post(url, params={"description": desc})
            status = res.status_code
            if status == 200:
                r1 = res.read()
            else:
                # Process error!
                pass
        except:
            traceback.print_exc()
        finally:
            if reply:
                res.close()
        return r1

    def list_files(self, experiment, dims, dbhandle=None):
        base = "http://samweb.fnal.gov:8480"
        url = "%s/sam/%s/api/files/list" % (base, experiment)
        flist = []
        with requests.Session() as sess:
            res = safe_get(sess, url, params={"dims": dims, "format": "json"}, dbhandle=dbhandle)
        if res:
            try:
                flist = res.json()
            except ValueError:
                pass
        return flist

    def count_files(self, experiment, dims, dbhandle=None):
        base = "http://samweb.fnal.gov:8480"
        url = "%s/sam/%s/api/files/count" % (base, experiment)
        count = 0
        with requests.Session() as sess:
            res = safe_get(sess, url, params={"dims": dims}, dbhandle=dbhandle)
        if res:
            text = res.content
            try:
                count = int(text)
            except ValueError:
                pass
        return count

    def count_files_list(self, experiment, dims_list):
        """
        """
        def getit(req, url):
            retries = 5
            r = req.get(url)
            while r and r.status_code >= 500 and retries > 0:
                time.sleep(5)
                retries = retries - 1
                r = req.get(url)
            if r:
                r.close()
            return r

        # if given an individual experiment, make it a list for
        # all the fetches
        if isinstance(experiment, basestring):
            experiment = [experiment] * len(dims_list)

        base = "http://samweb.fnal.gov:8480"
        urls = ["%s/sam/%s/api/files/count?%s" % (base, experiment[i],
                                                  urllib.urlencode({"dims": dims_list[i]})) for i in range(len(dims_list))]
        with requests.Session() as sess:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                #replies = executor.map(getit, urls)
                replies = executor.map(lambda url: getit(sess, url), urls)
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
        cherrypy.log.error("create_definition( %s, %s, %s )" % (experiment, name, dims))
        base = "https://samweb.fnal.gov:8483"
        path = "/sam/%s/api/definitions/create" % experiment
        url = "%s%s" % (base, path)
        res = None

        pdict = {"defname": name, "dims": dims, "user": "sam", "group": experiment}
        cherrypy.log.error("create_definition: calling: %s with %s " % (url, pdict))
        try:
            res = requests.post(url,
                                data=pdict,
                                verify=False,
                                cert=("%s/private/gsi/%scert.pem" % (os.environ["HOME"], os.environ["USER"]),
                                      "%s/private/gsi/%skey.pem" % (os.environ["HOME"], os.environ["USER"]))
                  )
            text = res.content
            cherrypy.log.error("definitions/create returns: %s" % text)
        except Exception as e:
            cherrypy.log.error("Exception creating definition: url %s args %s exception %s" % (url, pdict, e.args))
            return "Fail."
        finally:
            if res:
                res.close()
        return text

if __name__ == "__main__":
    import pprint
    sl = samweb_lite()
    print sl.create_definition("samdev", "mwm_test_%d" % os.getpid(), "(snapshot_for_project_name mwm_test_proj_1465918505)")
    i = sl.fetch_info("nova", "arrieta1-Offsite_test_Caltech-20160404_1157")
    i2 = sl.fetch_info("nova", "brebel-AnalysisSkimmer-20151120_0126")
    print "got:"
    pprint.pprint(i)
    print "got:"
    pprint.pprint(i2)

    l = sl.list_files("nova",
                      "file_name neardet_r00011388_s00_t00_S15-12-07_v1_data_keepup.caf.root,"
                      "neardet_r00011388_s00_t00_S15-12-07_v1_data_keepup.reco.root,"
                      "neardet_r00011388_s05_t00_S15-12-07_v1_data_keepup.reco.root,"
                      "neardet_r00011388_s05_t00_S15-12-07_v1_data_keepup.caf.root,"
                      "neardet_r00011388_s01_t00_S15-12-07_v1_data_keepup.reco.root,"
                      "neardet_r00011388_s01_t00_S15-12-07_v1_data_keepup.caf.root,"
                      "neardet_r00011388_s10_t00_S15-12-07_v1_data_keepup.reco.root")
    print("got list:")
    pprint.pprint(l)

    c = sl.count_files("nova",
                       "project_name 'vito-vito-calib-manual-Offsite-R16-01-27-prod2calib.e-neardet-20160210_1624',"
                       "'vito-vito-calib-manual-Offsite-R16-01-27-prod2calib.a-fardet-20160202_1814'")

    print "got count:", c

    l = sl.count_files_list("nova", ["defname:mwm_test_6", "defname:mwm_test_9", "defname:mwm_test_11"])
    print "got count list: ", l
