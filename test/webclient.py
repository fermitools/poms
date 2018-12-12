import time
import requests
import urllib.request, urllib.error, urllib.parse
import logging
import poms.test.utils as utils

logging.basicConfig(filename='test_requests.log',level=logging.DEBUG, filemode='w', format='%(asctime)s %(message)s')


class WebClient(object):
    def __init__(self, base_url=None):
        self.rs = requests.Session()
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = utils.get_base_url()


    def get(self, url):
        return self.post(url, method='GET')


    def post(self, url, data=None, method='POST'):
        self.url = self.base_url + url
        try:
            if method == 'POST':
                t0 = time.time()
                response = self.rs.post(self.url, data = data)
                self.text = response.text
                self.code = response.status_code
                self.response_headers = response.headers()
                response.close()
                duration = time.time() - t0
            else:
                t0 = time.time()
                
                response = self.rs.get(self.url)
                self.code = response.status_code
                self.text = response.text
                self.response_headers = response.headers
                response.close()
                duration = time.time() - t0
            
            logging.info("url: " + self.url + "  data: " + str(data) + "  method: " + method + "  status: " +str(self.code) + "  response_time: " + str(duration) )
            if self.code != 200:
                logging.info("error text: " + self.text)
                
        except urllib.error.HTTPError as error:
            duration = time.time() - t0
            response = error
            self.code = response.status_code
            logging.info("url: " + self.url + "  data: " + str(data) + "  method: " + method + "  status: " +str(self.code) + "  response_time: " + str(duration) )
            response.close()
