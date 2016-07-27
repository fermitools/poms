import requests
import pprint
import json


class Elasticsearch:

    def __init__(self):
        self.base_url="https://fifemon-es.fnal.gov"
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

    es = Elasticsearch()

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

