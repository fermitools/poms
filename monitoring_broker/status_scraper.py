#!/usr/bin/python

from ConfigParser import SafeConfigParser
import re
import urllib2
import httplib
import traceback
import os
import sys
import ssl,socket

import pycurl
from StringIO import StringIO


class status_scraper():

    def __init__(self,configfile, poms_url):
        self.poms_url = poms_url
        defaults = { "subservices" : "", "scrape_url":"" , "scrape_regex":"", "percent":"100", "scrape_match_1":"", "scrape_bad_match_1":"", "debug":"0"}
        self.cf = SafeConfigParser(defaults)
        self.cf.read(configfile)
        self.flush_cache()
        self.percents = {}
        self.parent = {}
        self.url = {}
        self.debug = int(self.cf.get('global','debug'))
        
    def flush_cache(self):
        self.page_cache = {}
        self.status = {}

    def fetch_page(self, url):
        if not self.page_cache.has_key(url):
            if self.debug: print "fetching page: ", url
            try:
                lines = []
                c = pycurl.Curl()
                buffer = StringIO()
                c.setopt(c.URL, url)
                c.setopt(c.WRITEFUNCTION, buffer.write)
                c.setopt(c.SSLCERT,"/tmp/x509up_u%d" % os.getuid())
                c.setopt(c.SSLKEY,"/tmp/x509up_u%d" % os.getuid())
                c.setopt(c.SSL_VERIFYHOST, 0)
                c.setopt(c.HTTPHEADER, ['Accept: application/json','Accept: text/plain','Accept: text/html'])
                c.perform()
                c.close()
                self.page_cache[url] = buffer.getvalue().split("\n")
                buffer.close()
                if self.debug: print "Fetched."
                if self.debug: print self.page_cache[url]
            except Exception as e:
                print "Ouch! "
                print traceback.format_exc()
                if self.page_cache.has_key(url):
                    del self.page_cache[url]
                return None
        return self.page_cache[url]

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
            if rs == 'bad':
               n_bad = n_bad + 1

        percent = int(self.cf.get(section,'percent'))

        if n_good + n_bad > 0:
            self.percents[section] = ((n_good ) * 100.0 / (n_good+n_bad)) 
        else:
            self.percents[section] = -1

        #print "recurse: ", s , self.percents[s] , "%"

	if n_good == 0 and n_bad == 0:
            self.status[section] = 'unknown'
	elif (self.percents[section] < percent):
            self.status[section] = 'bad'
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
            scrape_url = self.cf.get(s,'scrape_url')
            self.url[s] = scrape_url
            scrape_regex = self.cf.get(s,'scrape_regex')
            good = self.cf.get(s,'scrape_match_1')
            bad = self.cf.get(s,'scrape_bad_match_1')
            percent = int(self.cf.get(s,'percent'))
            n_good = 0
            n_bad = 0
            if scrape_url and scrape_regex:
	        if self.debug: print "scraping %s for matches" % scrape_url
                re_obj = re.compile(scrape_regex)
                lines = self.fetch_page(scrape_url)
                if not lines:
                    continue
                for line in lines :
                    if self.debug: print "got:", line
                    m = re_obj.search(line)
                    if m:
                        if m.group(1) == good:
                             if self.debug: print "good"
                             n_good = n_good + 1 

                        if m.group(1) == bad:
                             if self.debug: print "bad"
                             n_bad = n_bad + 1 

                if n_good + n_bad > 0:
                    self.percents[s] = (n_good ) * 100.0 / (n_good+n_bad) 
                else:
                    self.percents[s] = -1

                if len(lines) == 0 or (n_good == 0 and n_bad == 0):
                    self.status[s] = 'unknown'
	        else:
                    if n_good == 0 or self.percents[s] < percent:
	 	        self.status[s] = 'bad'
	            else:
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
        try:
            while 1:
                self.one_pass()
                time.sleep(300)
        except:
            print "Interuppted. Quitting"

    def report(self):
        # XXX this needs to actually make a page and post to webservice
        for s in self.cf.sections():
            if s == 'global':
                 continue
            print "service %s has status %s percent %d kids %s\n" % ( s, self.status[s], self.percents[s], self.cf.get(s, "subservices"))
            # this should POST, but for now this is easier to debug
            name = s.replace("service ","")
            parent = self.parent.get(s,'').replace("service ","")
            report_url =self.poms_url + "/update_service?name=%s&status=%s&parent=%s&host_site=%s" % (name, self.status[s], parent, self.url.get(s,''))
            print "trying: " , report_url
            c = urllib2.urlopen(report_url)
            print c.read()
            c.close()

ss = status_scraper("status_scraper.cfg", "http://fermicloud045.fnal.gov:8080/poms")
ss.one_pass()
