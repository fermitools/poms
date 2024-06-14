#!/usr/bin/env python

from collections import deque
from logging import config
import urllib.request
import urllib.parse
import urllib.error
import time
import datetime
import concurrent.futures
import sys
import os
import traceback
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from poms.webservice.utc import utc
from poms.webservice.poms_model import FaultyRequest
import poms.webservice.logit as logit
from toml_parser import TConfig
#import configparser

#config = configparser.ConfigParser()
#config.read(os.environ["WEB_CONFIG"])
config = TConfig()

# shut up annoying InsecureRequestWarnings
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


def safe_get(sess, url, *args, **kwargs):
    # TODO: Need more refactoring to optimize
    reply = None
    dbh = kwargs.pop("dbhandle", None)

    if url == None:
        return None

    if dbh is None:
        try:
            sess.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.2)))
            # Timeout may need adjustment!
            reply = sess.get(url, timeout=5.0, verify=False, *args, **kwargs)
            if reply.status_code != 200:
                logit.log("ERROR", "Error: got status %d for url %s" % (reply.status_code, url))
                return None  # No need to return the response
        except BaseException:
            # Severe errors like network or DNS problems.
            logit.log(logit.ERROR, "Died in safe_get:" + url)
            logit.log(logit.ERROR, traceback.format_exc())
            return None  # No need to return the response
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
        sess.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.2)))
        # Timeout may need adjustment!
        reply = sess.get(url, timeout=5.0, verify=False, *args, **kwargs)
        # SAM returns 400 when dataset definition isn't found calling files/count, so treat that as a 404
        if reply.status_code not in [200, 400, 404]:
            logit.log("ERROR", "Error: got status %d for url %s" % (reply.status_code, url))
            # Process error, store faulty query in DB
            fault = FaultyRequest(url=url, status=reply.status_code, message=reply.reason)
            dbh.add(fault)
            dbh.commit()
            return None  # No need to return the response
    except BaseException:
        # Process error!
        # Severe errors like network or DNS problems.
        logit.log(logit.ERROR, "Died in safe_get:" + url)
        logit.log(logit.ERROR, traceback.format_exc())
        # Do the same?
        fault = FaultyRequest(url=reply.url, status=reply.status_code, message=reply.reason)
        dbh.add(fault)
        dbh.commit()
        return None  # No need to return the response
    finally:
        if reply:
            reply.close()
    # Everything went OK
    return reply


class proj_class_dict(dict):
    # keep a separate type for project dicts for bookkeeping
    pass


class samweb_lite:
    def __init__(self):
        self.proj_cache = {}
        self.proj_cache_time = {}
        self.valid = 300

    def flush(self):
        self.proj_cache = None
        self.proj_cache_time = None
        self.valid = 0

    def have_cache(self, experiment, projid):
        if not self.proj_cache_time or not self.proj_cache:
            return 0
        s = self.proj_cache_time.get(experiment + projid, 0)
        p = self.proj_cache.get(experiment + projid, None)

        if p and (time.time() - s < self.valid or p["project_status"] == "completed"):
            return 1

        return 0

    def take_snapshot(self, experiment, defname):
        """
           basic samweb snapshot interface
        """
        if not experiment or not defname or defname == "None":
            return -1

        res = None
        base = "%s" % config.get("SAM", "sam_base")
        url = "%s/sam/%s/api/definitions/name/%s/snapshot" % (base, experiment, defname)
        retries = 3
        for i in range(retries + 1):
            logit.log("take_snapshot try %d" % i)
            try:
                with requests.Session() as sess:
                    res = sess.post(
                        url,
                        data={"group": experiment},
                        verify=False,
                        cert=(
                            "%s/private/gsi/%scert.pem" % (os.environ["HOME"], os.environ["USER"]),
                            "%s/private/gsi/%skey.pem" % (os.environ["HOME"], os.environ["USER"]),
                        ),
                    )
                    res.raise_for_status()
                break
            except Exception as e:
                logit.log("ERROR", "Exception taking snapshot: %s" % e)
                if i == retries:
                    raise

            time.sleep(1)

        if res:
            return int(res.text)
        else:
            return -1

    def recovery_dimensions(self, experiment, projid, useprocess=0, dbhandle=None):
        if not experiment or not projid or projid == "None":
            return ""

        base = "%s" % config.get("SAM", "sam_base")
        url = "%s/sam/%s/api/projects/name/%s/recovery_dimensions?useProcess=%s" % (base, experiment, projid, useprocess)
        with requests.Session() as sess:
            res = safe_get(sess, url, dbhandle=dbhandle)
        # default to basic consumed status
        info = "project_name %s minus consumed_status co%%" % projid
        if res:
            info = res.text

        return info

    def fetch_info(self, experiment, projid, dbhandle=None):
        if not experiment or not projid or projid == "None":
            return {}

        if self.have_cache(experiment, projid):
            return self.proj_cache[experiment + projid]

        base = "%s" % config.get("SAM", "sam_base")
        url = "%s/sam/%s/api/projects/name/%s/summary?format=json&process_limit=0" % (base, experiment, projid)
        with requests.Session() as sess:
            res = safe_get(sess, url, dbhandle=dbhandle)
        info = {}
        if res:
            info = proj_class_dict(res.json())
            self.do_totals(info)
            if not self.proj_cache:
                self.proj_cache = {}
            if not self.proj_cache_time:
                self.proj_cache_time = {}
            self.proj_cache[experiment + projid] = info
            self.proj_cache_time[experiment + projid] = time.time()
        return info

    def fetch_info_list(self, task_list, dbhandle=None, urls=None):
        # ~ return [ {"tot_consumed": 0, "tot_unknown": 0, "tot_jobs": 0, "tot_jobfails": 0} ] * len(task_list)    #VP Debug
        if not urls:
            base = "%s" % config.get("SAM", "sam_base").replace("\"", "")
            urls = [
                "%s/sam/%s/api/projects/name/%s/summary?format=json&process_limit=0"
                % (base, s.campaign_stage_snapshot_obj.experiment, s.project)
                if s.project and s.project != "None"
                else None
                for s in task_list
            ]
        with requests.Session() as sess:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                # replies = executor.map(sess.get, urls)
                replies = executor.map(lambda url: safe_get(sess, url, dbhandle=dbhandle), urls)
                # Replies is a generator that gets consumed once you start iterating over it in the for loop. Once it's exhausted, we cannot iterate over it again. 
                # This causses an error "cannot schedule new futures after shutdown" when we try to iterate over replies.
                # To fix this, we convert replies to a list before the for loop so that we can iterate over it as needed.
                replies_list = list(replies)
                executor.shutdown(wait=True)  # Wait for all tasks to complete and then shut down the executor
        
        infos = deque()
        
        for r in replies_list:
            if r:
                try:
                    info = proj_class_dict(r.json())
                    self.do_totals(info)
                    infos.append(info)
                except BaseException:
                    # Error in JSON parsing
                    logit.log(logit.ERROR, "Died in fetch_info_list:")
                    logit.log(logit.ERROR, traceback.format_exc())
                    infos.append({})
            else:
                infos.append({})
        return infos

    def do_totals(self, info):
        if not info.get("processes", None):
            info["tot_jobs"] = info.get("process_counts", {}).get("completed", 0)
            info["tot_consumed"] = info.get("file_counts", {}).get("consumed", 0) + info.get("file_counts", {}).get(
                "completed", 0
            )
            info["tot_cancelled"] = info.get("file_counts", {}).get("cancelled", 0)
            info["tot_failed"] = info.get("file_counts", {}).get("failed", 0)
            info["tot_delivered"] = info.get("file_counts", {}).get("delivered", 0)
            info["tot_unknown"] = info.get("file_counts", {}).get("unknown", 0)
            return
        tot_consumed = 0
        tot_cancelled = 0
        tot_skipped = 0
        tot_delivered = 0
        tot_unknown = 0
        tot_failed = 0
        tot_jobs = 0
        tot_jobfails = 0
        for proc in info["processes"]:
            tot_consumed += proc["counts"]["consumed"]
            tot_cancelled += proc["counts"]["cancelled"]
            tot_failed += proc["counts"]["failed"]
            tot_skipped += proc["counts"].get("skipped", 0)
            tot_delivered += proc["counts"].get("delivered", 0)
            tot_unknown += proc["counts"].get("unknown", 0)
            tot_jobs += 1
            if proc["status"] != "completed":
                tot_jobfails += 1

        info["tot_consumed"] = tot_consumed
        info["tot_cancelled"] = tot_cancelled
        info["tot_failed"] = tot_failed
        info["tot_skipped"] = tot_skipped
        info["tot_jobs"] = tot_jobs
        info["tot_jobfails"] = tot_jobfails
        info["tot_delivered"] = tot_delivered
        info["tot_unknown"] = tot_unknown
        # we don's need the individual process info, just the totals..
        if "processes" in info:
            del info["processes"]

    def update_project_description(self, experiment, projname, desc):
        base = "%s" % config.get("SAM", "sam_base")
        url = "%s/sam/%s/api/projects/%s/%s/description" % (base, experiment, experiment, projname)
        res = None
        r1 = None
        if projname is None or projname == "None":
            return
        try:
            res = requests.post(
                url,
                data={"description": desc},
                verify=False,
                cert=(
                    "%s/private/gsi/%scert.pem" % (os.environ["HOME"], os.environ["USER"]),
                    "%s/private/gsi/%skey.pem" % (os.environ["HOME"], os.environ["USER"]),
                ),
            )
            status = res.status_code
            if status == 200:
                r1 = res.text
            else:
                # Process error!
                r1 = res.text
                pass
        except BaseException:
            logit.log(logit.ERROR, "Died in update_project_description :" + url)
            logit.log(logit.ERROR, traceback.format_exc())
        finally:
            if res:
                res.close()
        return r1

    def cleanup_dims(self, dims):
        # the code currently generates some useless bits..
        dims = dims.replace("file_name '%' and ", "")
        dims = dims.replace("union (file_name __located__ )", "")
        dims = dims.replace("union (file_name __no_project__ )", "")
        dims = dims.replace("(file_name __located__ ) union", "")
        dims = dims.replace("(file_name __no_project__ ) union", "")
        return dims

    def get_metadata(self, experiment, filename):
        base = "%s" % config.get("SAM", "sam_base")
        url = "%s/sam/%s/api/files/name/%s/metadata?format=json" % (base, experiment, filename)
        res = requests.get(url, verify=False)
        try:
            return res.json()
        except BaseException:
            return {}

    def plain_list_files(self, experiment, dims):
        base = "%s" % config.get("SAM", "sam_base")
        url = "%s/sam/%s/api/files/list" % (base, experiment)
        flist = []
        dims = self.cleanup_dims(dims)
        res = requests.get(url, params={"dims": dims, "format": "json"}, verify=False)
        # print( "status code: %d url: %s" % (res.status_code, res.url))
        # print( "got output: %s" % res.text)
        if res.status_code >= 200 and res.status_code < 300:
            logit.log(logit.INFO, "got status code %d looking up dims: %s" % (res.status_code, dims))
        else:
            logit.log(logit.ERROR, "got status code %d looking up dims: %s" % (res.status_code, dims))
        try:
            flist = res.json()
        except ValueError:
            flist = []
        return flist

    def list_files(self, experiment, dims, dbhandle=None):
        base = "%s" % config.get("SAM", "sam_base")
        url = "%s/sam/%s/api/files/list" % (base, experiment)
        flist = deque()
        dims = self.cleanup_dims(dims)
        with requests.Session() as sess:
            res = safe_get(sess, url, params={"dims": dims, "format": "json"}, dbhandle=dbhandle)
        if res:
            try:
                flist = res.json()
            except ValueError:
                pass
        return flist

    def count_files(self, experiment, dims, dbhandle=None):
        logit.log("INFO", "count_files(experiment=%s, dims=%s)" % (experiment, dims))
        base = "%s" % config.get("SAM", "sam_base")
        url = "%s/sam/%s/api/files/count" % (base, experiment)
        dims = self.cleanup_dims(dims)
        count = -1
        # print("count_files(experiment=%s, dims=%s, url=%s)" % (experiment, dims,url))
        with requests.Session() as sess:
            res = safe_get(sess, url, params={"dims": dims}, dbhandle=dbhandle)
        if res:
            # print("Got status: %d" % res.status_code)
            if res.status_code != 200:
                logit.log("ERROR", "Error in samweb_lite.count_files, got status %d" % res.status_code)
            text = res.content
            try:
                count = int(text)
            except ValueError:
                pass
        return count

    def count_files_list(self, experiment, dims_list):
        def getit(req, url):
            retries = 10
            r = req.get(url, verify=False)
            while r and r.status_code >= 500 and retries > 0:
                time.sleep(1)
                retries = retries - 1
                r = req.get(url, verify=False)
            if r:
                r.close()
            return r

        # if given an individual experiment, make it a list for
        # all the fetches
        if isinstance(experiment, str):
            experiment = [experiment] * len(dims_list)

        base = "%s" % config.get("SAM", "sam_base").replace("\"", "")
        urls = [
            "%s/sam/%s/api/files/count?%s"
            % (base, experiment[i], urllib.parse.urlencode({"dims": self.cleanup_dims(dims_list[i])}))
            for i in range(len(dims_list))
        ]
        replies = None
        with requests.Session() as sess:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                # replies = executor.map(getit, urls)
                replies = executor.map(lambda url: getit(sess, url), urls)
                # Replies is a generator that gets consumed once you start iterating over it in the for loop. Once it's exhausted, we cannot iterate over it again. 
                # This causses an error "cannot schedule new futures after shutdown" when we try to iterate over replies.
                # To fix this, we convert replies to a list before the for loop so that we can iterate over it as needed.
                replies_list = list(replies)
                executor.shutdown(wait=True)  # Wait for all tasks to complete and then shut down the executor
        
        infos = deque()
        if replies_list:
            try:
                for r in replies_list:
                    if r.text.find("query limit") > 0:
                        infos.append(-1)
                    else:
                        try:
                            infos.append(int(r.text))
                        except BaseException as b:
                            logit.log("Exception %s converting count_files response to int: %s" % (b, r.text))
                            infos.append(-1)
            except Exception as e:
                logit.log("Exception %s getting replies: %s" % (e, repr(replies)))
        logit.log("URLS: %s" % urls)
        return infos

    def create_definition(self, experiment, name, dims):
        logit.log("INFO", "create_definition( %s, %s, %s )" % (experiment, name, dims))
        base = "%s" % config.get("SAM", "sam_base")
        path = "/sam/%s/api/definitions/create" % experiment
        url = "%s%s" % (base, path)
        res = None

        pdict = {"defname": name, "dims": dims, "user": "sam", "group": experiment}
        logit.log("INFO", "create_definition: calling: %s with %s " % (url, pdict))
        text = None
        retries = 3
        for i in range(retries + 1):
            logit.log("create_defintition try %d" % i)
            try:
                with requests.Session() as sess:
                    res = sess.post(
                        url,
                        data=pdict,
                        verify=False,
                        cert=(
                            "%s/private/gsi/%scert.pem" % (os.environ["HOME"], os.environ["USER"]),
                            "%s/private/gsi/%skey.pem" % (os.environ["HOME"], os.environ["USER"]),
                        ),
                    )
                    res.raise_for_status()

                    text = res.content
                    logit.log("INFO", "definitions/create returns: %s" % text)
                    break
            except Exception as e:
                logit.log("ERROR", "Exception creating definition: url %s args %s exception %s" % (url, pdict, e.args))
                # 409 error means its already defined..
                # check if the definition exists and remove it
                if e.args[0][0:3] == "409":
                    # delete the existing SAM definition
                    self.remove_existing_definition(experiment, name)
                    # recreate the SAM definition with updated dimensions
                    self.create_definition(experiment, name, dims)
                    break
                elif i == retries:
                    raise

            time.sleep(5)

        return text

    def remove_existing_definition(self, experiment, name):
        logit.log("INFO", "remove_existing_definition( %s, %s )" % (experiment, name))
        base = "%s" % config.get("SAM", "sam_base")
        path = "/sam/%s/api/definitions/name/%s" % (experiment, name)
        url = "%s%s" % (base, path)
        res = None
        logit.log("INFO", "remove_existing_definition: calling: %s " % url)
        text = None
        try:
            with requests.Session() as sess:
                logit.log("INFO", "GET definitions/name %s" % name)
                # check the definition exists
                res = sess.get(url, verify=False)
                res.raise_for_status()

                text = res.content
                # reset "sess", otherwise the delete call will miss the cert
                sess.close()
                logit.log("INFO", "GET definitions/name returns: %s" % text)
                logit.log("INFO", "DELETE definitions/name %s" % name)
                # delete the preexisting SAM definition
                res = sess.delete(
                    url,
                    verify=False,
                    cert=(
                        "%s/private/gsi/%scert.pem" % (os.environ["HOME"], os.environ["USER"]),
                        "%s/private/gsi/%skey.pem" % (os.environ["HOME"], os.environ["USER"]),
                    ),
                )
                res.raise_for_status()

                text = res.content
                logit.log("INFO", "DELETE definitions/name %s returns: %s" % (name, text))
        except Exception as e:
            logit.log("ERROR", "Exception for definition: url %s exception %s" % (url, e.args))
            raise
        return text


if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings()

    import pprint

    sl = samweb_lite()

    print(sl.recovery_dimensions("samdev", "mengel-fife_wrap_20180927_153506_1003267", useprocess=1))

    md = sl.get_metadata(
        "uboone", "PhysicsRun-2016_1_3_13_5_49-0004357-00974_20180929T225108_numi_unbiased_20180930T032518_merged.root"
    )
    pprint.pprint(md)

    r1 = sl.update_project_description("samdev", "mengel-fife_wrap_20170701_102228_3860387", "test_1234")
    print("got result:", r1)
    i = sl.fetch_info("samdev", "mengel-fife_wrap_20170701_102228_3860387")
    print("got result:", i)
    res = sl.take_snapshot("nova", "mwm_test_6")
    print("got snapshot id %s" % res)
    sys.exit(0)

    print(sl.create_definition("samdev", "mwm_test_%d" % os.getpid(), "(snapshot_for_project_name mwm_test_proj_1465918505)"))
    print(sl.remove_existing_definition("samdev", "mwm_test_%d" % os.getpid()))
    i = sl.fetch_info("nova", "arrieta1-Offsite_test_Caltech-20160404_1157")
    i2 = sl.fetch_info("nova", "brebel-AnalysisSkimmer-20151120_0126")
    print("got:")
    pprint.pprint(i)
    print("got:")
    pprint.pprint(i2)

    l = sl.list_files(
        "nova",
        "file_name neardet_r00011388_s00_t00_S15-12-07_v1_data_keepup.caf.root,"
        "neardet_r00011388_s00_t00_S15-12-07_v1_data_keepup.reco.root,"
        "neardet_r00011388_s05_t00_S15-12-07_v1_data_keepup.reco.root,"
        "neardet_r00011388_s05_t00_S15-12-07_v1_data_keepup.caf.root,"
        "neardet_r00011388_s01_t00_S15-12-07_v1_data_keepup.reco.root,"
        "neardet_r00011388_s01_t00_S15-12-07_v1_data_keepup.caf.root,"
        "neardet_r00011388_s10_t00_S15-12-07_v1_data_keepup.reco.root",
    )
    print("got list:")
    pprint.pprint(l)

    cs = sl.count_files(
        "nova",
        "project_name 'vito-vito-calib-manual-Offsite-R16-01-27-prod2calib.e-neardet-20160210_1624',"
        "'vito-vito-calib-manual-Offsite-R16-01-27-prod2calib.a-fardet-20160202_1814'",
    )

    print("got count:", cs)

    l = sl.count_files_list("nova", ["defname:mwm_test_6", "defname:mwm_test_9", "defname:mwm_test_11"])
    print("got count list: ", l)
