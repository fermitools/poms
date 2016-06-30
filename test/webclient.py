import time
import urllib2
import urllib
import unittest
import logging


logging.basicConfig(filename='test_requests.log',level=logging.DEBUG, filemode='w', format='%(asctime)s %(message)s')


class WebClient(object):
    def __init__(self, base_url=''):
        self.base_url = base_url


    def get(self, url):
        return self.post(url, method='GET')


    def post(self, url, data=None, method='POST'):
        self.url = self.base_url + url
        try:
            if method == 'POST':
                t0 = time.time()
                data = urllib.urlencode(data)
                request = urllib2.Request(self.url, data)
                response = urllib2.urlopen(request)
                self.code = response.getcode()
                self.text = response.read()
                self.response_headers = dict(response.info())
                duration = time.time() - t0
            else:
                t0 = time.time()
                request = urllib2.Request(self.url)
                response = urllib2.urlopen(request)
                self.code = response.getcode()
                self.text = response.read()
                self.response_headers = dict(response.info())
                duration = time.time() - t0
            
            logging.info("url: " + self.url + "  data: " + str(data) + "  method: " + method + "  status: " +str(self.code) + "  response_time: " + str(duration) )
                
        except urllib2.HTTPError, error:
            duration = time.time() - t0
            response = error
            self.code = response.getcode()
            logging.info("url: " + self.url + "  data: " + str(data) + "  method: " + method + "  status: " +str(self.code) + "  response_time: " + str(duration) )

        
