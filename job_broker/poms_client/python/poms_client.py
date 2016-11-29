#!/usr/bin/env python

import urllib
import urllib2
import os

def register_poms_campaign(campaign_name, user = None, experiment = None, version = None, dataset = None, campaign_definition = None, test = None):
    return int(
    make_poms_call( 
	method = 'register_poms_campaign',
	campaign_name = campaign_name,
	user = user,
	experiment = experiment,
	version = version,
	dataset = dataset,
	campaign_definition = campaign_definition,
	test = test).replace('Campaign=',''))

def get_task_id_for(campaign, user = None, command_executed = None, input_dataset = None, parent_task_id = None, test = None, experiment = None):

    return int(
       make_poms_call( 
         method = 'get_task_id_for',
	 campaign = campaign,
	 user = user,
	 command_executed = command_executed,
	 input_dataset = input_dataset,
	 parent_task_id = parent_task_id,
	 test = test).replace('Task=','')
        )

def make_poms_call(**kwargs):

    method = kwargs.get("method")
    del kwargs["method"]


    if kwargs.get("test"):
        base='http://fermicloud045.fnal.gov:8080/poms/'
        del kwargs["test"]
    else:
        base='http://pomsgpvm01.fnal.gov:8080/poms/'


    for k in kwargs.keys():
        if kwargs[k] == None:
            del kwargs[k]

    if os.environ.get("POMS_CLIENT_DEBUG", None):
        print "poms_client: making call %s( %s ) at %s" % (method, kwargs, base)

    c = urllib2.urlopen("%s/%s" % (base,method), urllib.urlencode(kwargs));
    res = c.read()
    print res
    return res
  


if __name__ == '__main__':
    # simple tests...
    res = make_poms_call(test=True, method="active_jobs")
    tid = get_task_id_for(test = True, campaign=14, command_executed="fake test job")
    cid = register_poms_campaign("mwm_client_test",  user = "mengel", experiment = "samdev", version = "v0_0", dataset = "mwm_test_data", test = True)
    print "got task id ", tid
    print "got campaign id ", cid

