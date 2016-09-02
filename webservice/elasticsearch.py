import requests
import pprint
import json
import cherrypy
from datetime import datetime

class Elasticsearch:

    def __init__(self, debug=0):
        if debug == 1:
            self.base_url="https://fifemon-es.fnal.gov"
            #self.base_url="http://sammongpvm01.fnal.gov:9200"
        else:
            self.base_url=cherrypy.config.get('elasticsearch_base_url').strip('"')

        self.cert=cherrypy.config.get('elasticsearch_cert').strip('"')
        self.key=cherrypy.config.get('elasticsearch_key').strip('"')

        requests.packages.urllib3.disable_warnings()

    def search(self, **kwargs):
        url_bits = []
        url_bits.append(self.base_url)

        if kwargs.get('index'):
            index = kwargs.get('index')
            url_bits.append(index)

        if kwargs.get('types'):
            types = ",".join(kwargs.get('types'))
            url_bits.append(types)

        search_handle = "_search"

        url_bits.append(search_handle)

        url = "/".join(url_bits)

        r = requests.get(url, data=json.dumps(kwargs.get('query')), verify=False)
        #print r.url
        #print r.json
        return json.loads(r.text)

    def index(self, index, doc_type, body):
        timestamp = body.get("timestamp")
        if isinstance(timestamp, datetime):
            timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%S')
            body["timestamp"] = timestamp
        url_bits = [self.base_url, index, doc_type]
        url = "/".join(url_bits)
        payload = json.dumps(body)
        r = requests.post(url, data=payload, cert=(self.cert, self.key), verify=False)
        #print r.text
        #print r.status_code
        return r.json()



if __name__ == '__main__':
    #example of a type is ifdh

    #current indicies:
    '''
    fife-dh-*
    fife-events
    fifebatch-logs-*
    fifebatch-slots
    fifemon-logs-*
    lpc-jobs
    '''

    es = Elasticsearch(debug=1)

    #example of basic searching and getting particular fields from object
    query = {
        'query' : {
            'term' : { 'jobid' : '9034906.0@fifebatch1.fnal.gov' }
        }
    }

    response= es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
    pprint.pprint(response)

    print "# of records: ", response.get('hits').get('total')
    print "# of ms to query: ", response.get('took')
    for record in response.get('hits').get('hits'):
        print record.get('_source').get('jobid')
        print record.get('_source').get('event_message')
        print "*" * 13

    print "*" * 100

    #emample of searching by field
    query = {
        "fields" : ["jobid", "Owner"],
        "query" : {
            "term" : { "jobid" : "9034906.0@fifebatch1.fnal.gov" }
        }
    }
    response = es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
    pprint.pprint(response)

    print "*" * 100

    #example of iterating the result set
    query = {
        'query' : {
            'term' : { 'jobid' : '9034906.0@fifebatch1.fnal.gov' }
        }
    }

    response = es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)

    for record in response.get('hits').get('hits'):
        for k,v in record.get("_source").iteritems():
            print str(k) + ": " + str(v)
        print "---"

    print "*" * 100

    #example of specifying a from and a size which can be useful for pagination
    query = {
        "from" : 0, "size" : 3,
        'query' : {
            'term' : { 'jobid' : '9034906.0@fifebatch1.fnal.gov' }
        }
    }
    response = es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
    pprint.pprint(response)

    print "*" * 100

    #example of sorting by date
    query = {
        "sort" : [{ "@timestamp" : {"order" : "asc"}}],
        'query' : {
            'term' : { 'jobid' : '9034906.0@fifebatch1.fnal.gov' }
        }
    }
    response = es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
    pprint.pprint(response)

    print "*" * 100

    #example of sending a record where datetimes will be serialized
    payload = {'timestamp': datetime.now(), 'message': 'trying out elasticsearch with poms', 'user': 'mgheith'}
    response = es.index(index='poms-2016-09-02', doc_type='my-poms-type', body=payload)
    pprint.pprint(response)

    print "*" * 100

    #example of sending a record where datetimes will not be serialized
    payload = {'timestamp': '2016-09-02T20:00:00', 'message': 'trying out elasticsearch with poms not serialized', 'user': 'mgheith'}
    response = es.index(index='poms-2016-09-02', doc_type='my-poms-type', body=payload)
    pprint.pprint(response)
