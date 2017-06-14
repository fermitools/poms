#!/usr/bin/env python

from configparser import SafeConfigParser
import re
import requests
import urllib.request, urllib.parse, urllib.error
import traceback
import os
import sys
import ssl,socket
import time
import requests

from io import StringIO

# don't barf if we need to log utf8...
#import codecs
#sys.stdout = codecs.getwriter('utf8')(sys.stdout)

class status_scraper():

    def __init__(self,configfile, poms_url):
        self.poms_url = poms_url
        self.rs = requests.Session()
        self.rs.verify = False
        defaults = { "subservices" : u"", "scrape_url":u"" , "scrape_regex":u"", "percent":u"100", "scrape_match_1":u"", "scrape_warn_match_1":u"", "scrape_bad_match_1":u"", "debug":u"0", "multiline":None }
        self.cf = SafeConfigParser(defaults)
        # self.cf._strict = False
        self.cf.read(configfile)
        self.flush_cache()
        self.percents = {}
        self.warnpercents = {}
        self.totals = {}
        self.failed = {}
        self.warns = {}
        self.parent = {}
        self.url = {}
        self.debug = int(self.cf.get('global','debug'))
        if self.cf.has_option('global','cert'):
            os.environ['X509_USER_CERT'] = self.cf.get('global','cert')
            os.environ['X509_USER_KEY'] = self.cf.get('global','key')
        
    def flush_cache(self):
        self.page_cache = {}
        self.status = {}

    def fetch_page(self, url):
        if url not in self.page_cache:
            if self.debug: print("fetching page: ", url)
            try:
                lines = []
                cert = os.environ.get('X509_USER_CERT',"/tmp/x509up_u%d" % os.getuid())
                key = os.environ.get('X509_USER_KEY',"/tmp/x509up_u%d" % os.getuid())
                resp = self.rs.get(url, cert=(cert, key) )
                self.page_cache[url] = resp.text.split("\n")
                if self.debug: print("Fetched.")
                if self.debug: print(self.page_cache[url])
            except Exception as e:
                print("Ouch! ")
                print(traceback.format_exc())
                if url in self.page_cache:
                    del self.page_cache[url]
                return None
        return self.page_cache[url]

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
            scrape_url = self.cf.get(s,'scrape_url')
            self.url[s] = scrape_url
            scrape_regex = self.cf.get(s,'scrape_regex')
            good = self.cf.get(s,'scrape_match_1')
            if self.cf.has_option(s,'scrape_match_2'):
                good2 = self.cf.get(s,'scrape_match_2')
            else:
                good2 = None
            warn = self.cf.get(s,'scrape_warn_match_1')
            if self.cf.has_option(s,'scrape_warn_match_2'):
                warn2 = self.cf.get(s,'scrape_warn_match_2')
            else:
                warn2 = None
            bad = self.cf.get(s,'scrape_bad_match_1')
            if self.cf.has_option(s,'scrape_bad_match_2'):
                bad2 = self.cf.get(s,'scrape_bad_match_2')
            else:
                bad2 = None
            percent = int(self.cf.get(s,'percent'))
            if self.cf.has_option(s,'warnpercent'):
                warnpercent = int(self.cf.get(s,'warnpercent'))
            else:
                warnpercent = 0
            n_good = 0
            n_bad = 0
            n_warn = 0
            if scrape_url and scrape_regex:
                if self.debug: print("scraping %s for matches of ruleset %s" % (scrape_url, s))
                re_obj = re.compile(scrape_regex)
                lines = self.fetch_page(scrape_url)
                if not lines:
                    continue

                # for multiline match, smush them all together and just
                # check the whole block..
                if self.cf.get(s,'multiline'):
                    lines = [ '\n'.join(lines) ]

                for line in lines :
                    if self.debug: print("checking for %s : in %s" % (scrape_regex, line[:30]))
                    m = re_obj.search(line)
                    if m:
                        if self.debug: print(("m.group(0) is |%s|" % m.group(0)))
                        if self.debug: print(("m.group(1) is |%s|" % m.group(1)))
                        if good == "" or m.group(1) == good or (good2 and m.group(1) == good2):
                             if self.debug: print("good")
                             n_good = n_good + 1 

                        if (warn and m.group(1)) == warn or (warn2 and m.group(1) == warn2):
                             if self.debug: print("warn")
                             n_warn = n_warn + 1 

                        if (bad and m.group(1) == bad) or (bad2 and m.group(1) == bad2):
                             if self.debug: print("bad")
                             n_bad = n_bad + 1 
                    else:
                        if self.debug: print("no match!")

                if n_good + n_bad + n_warn > 0 :
                    self.percents[s] = (n_good ) * 100.0 / (n_good+n_bad+n_warn) 
                    self.warnpercents[s] = (n_warn ) * 100.0 / (n_good+n_bad+n_warn) 
                else:
                    self.percents[s] = -1
                    self.warnpercents[s] = -1

                if len(lines) == 0 or (n_good == 0 and n_bad == 0 and n_warn == 0):
                    self.status[s] = 'unknown'
                else:
                    if n_warn > 0 and n_bad == 0:
                        self.status[s] = 'degraded'
                    elif n_good == 0 and n_warn == 0 or self.percents[s] < percent:
                        self.status[s] = 'bad'
                    elif  self.percents[s] < warnpercent:
                        self.status[s] = 'degraded'
                    else:
                        self.status[s] = 'good'

                self.totals[s] = n_good + n_bad + n_warn
                self.warns[s] = n_warn
                self.failed[s] = n_bad
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
            print("service %s has status %s percent %d kids %s\n" % ( s, self.status[s], self.percents[s], self.cf.get(s, "subservices")))
            # this should POST, but for now this is easier to debug
            name = s.replace("service ","")
            parent = self.parent.get(s,'').replace("service ","")
            if self.cf.has_option(s,'description'):
                description = self.cf.get(s,'description')
            else:
                description = s
            report_url =self.poms_url + "/update_service?name=%s&status=%s&parent=%s&host_site=%s&total=%d&failed=%d&description=%s" % (name, self.status[s], parent, self.url.get(s,''), self.totals.get(s,0), self.failed.get(s,0), urllib.parse.quote_plus(description))
            print("trying: " , report_url)
            r = self.rs.get(report_url)
            print(r.text)
            r.close()

if __name__ == '__main__':

    #ss = status_scraper("status_scraper.cfg", "http://fermicloud045.fnal.gov:8080/poms")
    ss = status_scraper("status_scraper.cfg", "http://localhost:8080/poms")
    ss.poll()
