import requests
import pprint
import json

class Elasticsearch:

    def __init__(self):
        self.base_url="https://fifemon-es.fnal.gov"
        requests.packages.urllib3.disable_warnings()

    def search(self, index, types, query):
        types = ",".join(types)
        search_handle = "_search"

        url = "/".join((self.base_url, index, types, search_handle))

        r = requests.get(url, params=query, verify=False)
        return json.loads(r.text)


if __name__ == '__main__':
    #future example of a type is ifdh_logs
    es = Elasticsearch()
    query = {'jobid': '123456789@fifebatch.fnal.gov'}
    response = es.search(index="fifebatch-logs-2016.07.12", types=['condor_eventlog'], query=query)
    pprint.pprint(response)

