#!/usr/bin/env python

from ConfigParser import SafeConfigParser
import re
import urllib2

class status_scraper():

    def __init__(self,configfile):
        defaults = { "subservices" : "", "scrape_url":"" , "scrape_regex":"", "percent":"100", "scrape_match_1":"", "scrape_bad_match_1":""}
        self.cf = SafeConfigParser(defaults)
        self.cf.read(configfile)
        self.flush_cache()
        self.percents = {}
        
    def flush_cache(self):
        self.page_cache = {}
        self.status = {}

    def fetch_page(self, url):
        if not self.page_cache.has_key(url):
            try:
                lines = []
                f = urllib2.urlopen(url)
                self.page_cache[url] = f.readlines()
                f.close()
            except:
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
            scrape_url = self.cf.get(s,'scrape_url')
            scrape_regex = self.cf.get(s,'scrape_regex')
            good = self.cf.get(s,'scrape_match_1')
            bad = self.cf.get(s,'scrape_bad_match_1')
            percent = int(self.cf.get(s,'percent'))
            n_good = 0
            n_bad = 0
            if scrape_url and scrape_regex:
                re_obj = re.compile(scrape_regex)
                lines = self.fetch_page(scrape_url)
                for line in lines :
                    m = re_obj.search(line)
                    if m:
                        if m.group(1) == good:
                             n_good = n_good + 1 

                        if m.group(1) == bad:
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
            self.recurse(s)

        #
        # finally, report status
        #
        self.report()

    def report(self):
        # XXX this needs to actually make a page and post to webservice
        for s in self.cf.sections():
            print "service %s has status %s percent %d kids %s\n" % ( s, self.status[s], self.percents[s], self.cf.get(s, "subservices"))

ss = status_scraper("status_scraper.cfg")
ss.one_pass()
