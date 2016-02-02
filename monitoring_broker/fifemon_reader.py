#!/usr/bin/python

from ConfigParser import SafeConfigParser
import re
import urllib2
import urllib
import httplib
import traceback
import os
import sys
import ssl,socket
import time
import cookielib
import json

import pycurl
from StringIO import StringIO


class status_scraper():

    def __init__(self,configfile, poms_url):
        self.poms_url = poms_url
        defaults = { "subservices" : "", "target_path", "percent":"100", "upper": 90, "lower":80, "debug":"0"}
        self.cf = SafeConfigParser(defaults)
        self.cf.read(configfile)
        self.flush_cache()
        self.percents = {}
        self.totals = {}
        self.failed = {}
        self.parent = {}
        self.paths = {}
        self.url = self.cfg.get('global', 'fifemon_url')
        self.debug = int(self.cf.get('global','debug'))

        #
        # login initially...
        #
        self.cj = cookielib.CookieJar()
        self.opener = urllib2.build_opener(urllib2.HTTPCookeiProccessor(self.cj))
        login = self.cf.get('global','login')
        pw = self.cf.get('global','password')
        self.opener.open(self.url + "/login", {'login':login, 'password':pw})
        
    def flush_cache(self):
        self.page_cache = {}
        self.status = {}

    def fetch_item(self, path):
            
        if not self.page_cache.has_key(path):
            try:
                res = self.opener.open(self.url, {'target':path, 'from': '-10min', 'until':'-5min', 'format':'json'})
                self.page_cache[path] = json.loads(res.read())
                res.close()
            except Exception as e:
                print "Ouch! "
                print traceback.format_exc()
                if self.page_cache.has_key(url):
                    del self.page_cache[url]
                return None
        return self.page_cache[path]

    def recurse(self, section ):
        n_good = 0
        n_bad = 0
        if self.status.has_key(section):
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
           print "recurse: ", s , self.percents.get(s,0), "%"

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
            self.paths[s] = path
            low = self.cf.get(s,'low')
            high = self.cf.get(s,'high')

            if path:
                data = self.fetch_page(path)
                if data["datapoints"][0][0] > high:
                    self.status[s] = 'bad'
                if data["datapoints"][0][0] < low:
                    self.status[s] = 'good'
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
		print "Interupted. Quitting"

	    except:
	        print "Exception!"
	        traceback.print_exc()
	        pass
	    time.sleep(300)


    def report(self):
        # XXX this needs to actually make a page and post to webservice
        for s in self.cf.sections():
            if s == 'global':
                 continue
            print "service %s has status %s percent %d kids %s\n" % ( s, self.status[s], self.percents[s], self.cf.get(s, "subservices"))
            # this should POST, but for now this is easier to debug
            name = s.replace("service ","")
            parent = self.parent.get(s,'').replace("service ","")
            if self.cf.has_option(s,'description'):
                description = self.cf.get(s,'description')
            else:
                description = s
            report_url =self.poms_url + "/update_service?name=%s&status=%s&parent=%s&host_site=%s&total=%d&failed=%d&description=%s" % (name, self.status[s], parent, self.url.get(s,''), self.totals.get(s,0), self.failed.get(s,0), urllib.quote_plus(description))
            print "trying: " , report_url
            c = urllib2.urlopen(report_url)
            print c.read()
            c.close()

#ss = status_scraper("status_scraper.cfg", "http://fermicloud045.fnal.gov:8080/poms")
ss = status_scraper("status_scraper.cfg", "http://localhost:8080/poms")
ss.poll()

notes = """
<bel-kwinith>$ curl --data user=mengel --data password="xxxx" --cookie-jar /tmp/tcookies https://fifemon.fnal.gov/monitor/login
{"message":"Logged in"}
<bel-kwinith>$ curl --cookie /tmp/tcookies --data target=urls.samweb-nova.http-code --data from=-10min --data until=-5min --data format=json   https://fifemon.fnal.gov/monitor/api/datasources/proxy/1/render 
[{"target": "urls.samweb-nova.http-code", "datapoints": [[200.0, 1454444100]]}]
"""

