#!/usr/bin/env python

from configparser import SafeConfigParser
import re
import urllib.request, urllib.parse, urllib.error
import requests
import traceback
import os
import sys
import ssl,socket
import time
import http.cookiejar
import json

import pycurl
from io import StringIO

# don't whine about certificates
requests.packages.urllib3.disable_warnings()

# don't barf if we need to log utf8...
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)


class status_scraper:

    def __init__(self,configfile, poms_url):
        self.poms_url = poms_url
        defaults = { "subservices" : "", "path":"", "percent":"100", "upper": 90, "lower":80, "debug":"0", "above":None, "below":None, "warnabove":None, "warnbelow":None, 'source_url': ''}
        self.cf = SafeConfigParser(defaults)
        self.cf.read(configfile)
        pcfg = self.cf.get('global', 'passwdcfg')
        self.cf.read(pcfg)
        self.flush_cache()
        self.percents = {}
        self.totals = {}
        self.failed = {}
        self.parent = {}
        self.paths = {}
        self.session = requests.Session()
        self.source_urls = {}
        self.url = self.cf.get('global', 'fifemon_url')
        self.debug = int(self.cf.get('global','debug'))

        #
        # login initially...
        #
        #self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
        #self.do_login()

    def do_login(self, page):
	#
	# First stab at SSO stuff...
	#
        l1 = page.find('action="') + 8
        l2 = page.find('"',l1)
        path = page[l1:l2]
        print("got action= path: ", path)
        login = self.cf.get('global','login')
        pw = self.cf.get('global','password')
        res = self.session.post("https://pingprod.fnal.gov:9031%s" % path, data={"pf.username":login, "pf.pass": pw, "pf.ok": "clicked", "pf.cancel":""})
        page = res.text
        print("login gives ", repr(res.text))
        print("cookies:", self.session.cookies)
        
        #
        # this is brittle, we should use regex or something...
        #
        l1 = page.find("<form")
        
        l1 = page.find('action="', l1) +8
        l2 = page.find('"', l1)
        url = page[l1:l2]
        l1 = page.find('<input type="hidden" name="RelayState" value="')+46
        l2 = page.find('"', l1)
        rs = page[l1:l2]
        l1 = page.find('<input type="hidden" name="SAMLResponse" value="')+48
        l2 = page.find('"', l1)
        sr = page[l1:l2]
        res = self.session.post(url, data={"RelayState":rs, "SAMLResponse":sr})
        
    def flush_cache(self):
        self.page_cache = {}
        self.status = {}

    def fetch_item(self, path):
            
        if path not in self.page_cache:
            try:
                res = self.session.post(self.url+'/api/datasources/proxy/1/render', data = {'target':path, 'from': '-10min', 'until':'-5min', 'format':'json'}, verify=False)

                jdata = res.text
                #
                # First stab at SSO stuff...
                #
                if jdata.find("pingprod.fnal.gov") > 0:
                    self.do_login(jdata)
                    res = self.session.post(self.url+'/api/datasources/proxy/1/render', data = {'target':path, 'from': '-10min', 'until':'-5min', 'format':'json'}, verify=False)
                    jdata = res.text
                print("fetch for ", path, " yeilds: ", jdata)
                self.page_cache[path] = json.loads(jdata)
                res.close()
            except Exception as e:
                print("Ouch! ")
                print(traceback.format_exc())
                if path in self.page_cache:
                    del self.page_cache[path]
                return None
        return self.page_cache[path]

    def recurse(self, section ):
        n_good = 0
        n_bad = 0
        if section in self.status:
            return self.status[section]
        subservices = self.cf.get(section,'subservices').split(' ')
        res = 'good'
        for s in subservices:
            if s == '':
               continue
            s = "service " + s
            self.parent[s] = section
            rs = self.recurse(s)
            if rs == 'good':
               n_good = n_good + 1
            if rs == 'degraded':
               n_good = n_good + 0.5
               n_bad = n_bad + 0.5
            if rs == 'bad':
               n_bad = n_bad + 1


        percent = int(self.cf.get(section,'percent'))
        if self.cf.has_option(section,'warnpercent'):
            warnpercent = int(self.cf.get(section,'warnpercent'))
        else:
            warnpercent = 0

        self.totals[section] = int(n_good + n_bad)
        self.failed[section] = int(n_bad)

        if n_good + n_bad > 0:
            self.percents[section] = ((n_good ) * 100.0 / (n_good+n_bad)) 
        else:
            self.percents[section] = -1

        if self.debug:
           print("recurse: ", s , self.percents.get(s,0), "%")

	if n_good == 0 and n_bad == 0:
            self.status[section] = 'unknown'
	elif (self.percents[section] < percent):
            self.status[section] = 'bad'
	elif (self.percents[section] < warnpercent):
            self.status[section] = 'degraded'
        else:
            self.status[section] = 'good'

	return self.status[section]

    def one_pass(self):

        #
        # first check all the sections that have a url and
        # decide if they're bad or good.
        #
        slist = self.cf.sections()
        slist.sort()
        for s in slist:
            if s == 'global':
                 continue
            path = self.cf.get(s,'path')
            surl = self.cf.get(s,'source_url')
            self.source_urls[s] = surl

            if path:
		self.paths[s] = path
		low = self.cf.get(s,'above')
		high = self.cf.get(s,'below')
		wlow = self.cf.get(s,'warnabove')
		whigh = self.cf.get(s,'warnbelow')
                data = self.fetch_item(path)
                if data: 
		    self.status[s] = 'good'
		    if whigh and data[0]["datapoints"][0][0] > float(whigh):
                        print("degraded because ", data[0]["datapoints"][0][0], "above", warnhigh)
			self.status[s] = 'degraded'
		    if wlow and data[0]["datapoints"][0][0] < float(wlow):
                        print("degraded because ", data[0]["datapoints"][0][0], "below", warnlow)
			self.status[s] = 'degraded'
		    if high and data[0]["datapoints"][0][0] > float(high):
                        print("bad because ", data[0]["datapoints"][0][0], "above", high)
			self.status[s] = 'bad'
		    if low and data[0]["datapoints"][0][0] < float(low):
                        print("bad because ", data[0]["datapoints"][0][0], "below", low)
			self.status[s] = 'bad'
                else:
		    self.status[s] = "unknown"
        #
        # next check the ones that have sub-sections
        # to decide if they're bad or good.
        #
        for s in self.cf.sections():
            if s == 'global':
                 continue
            self.recurse(s)

        #
        # finally, report status
        #
        self.report()

    def poll(self):
        while 1:
            try:

                self.one_pass()

	    except KeyboardInterrupt:
		print("Interupted. Quitting")

	    except:
	        print("Exception!")
	        traceback.print_exc()
	        pass
	    time.sleep(300)


    def report(self):
        # XXX this needs to actually make a page and post to webservice
        for s in self.cf.sections():
            if s == 'global':
                 continue
            print("service %s has status %s percent %d kids %s\n" % ( s, self.status[s], self.percents.get(s,100), self.cf.get(s, "subservices")))
            # this should POST, but for now this is easier to debug
            name = s.replace("service ","")
            parent = self.parent.get(s,'').replace("service ","")
            if self.cf.has_option(s,'description'):
                description = self.cf.get(s,'description')
            else:
                description = s
            report_url =self.poms_url + "/update_service?name=%s&status=%s&parent=%s&host_site=%s&total=%d&failed=%d&description=%s" % (name, self.status[s], parent, self.source_urls.get(s,''), self.totals.get(s,0), self.failed.get(s,0), urllib.parse.quote_plus(description))
            print("trying: " , report_url)
            retries = 0
            while retries < 3:
                try:
                    c = self.session.get(report_url) 
                    break
                except:
                    retries = retries + 1
	            traceback.print_exc()
            print(c.text)
            c.close()

if __name__ == '__main__':
    ss = status_scraper("fifemon_reader.cfg", "http://localhost:8080/poms")
    ss.poll()

    notes = """
<bel-kwinith>$ curl --data user=mengel --data password="xxxx" --cookie-jar /tmp/tcookies https://fifemon.fnal.gov/monitor/login
{"message":"Logged in"}
<bel-kwinith>$ curl --cookie /tmp/tcookies --data target=urls.samweb-nova.http-code --data from=-10min --data until=-5min --data format=json   https://fifemon.fnal.gov/monitor/api/datasources/proxy/1/render 
[{"target": "urls.samweb-nova.http-code", "datapoints": [[200.0, 1454444100]]}]
"""

