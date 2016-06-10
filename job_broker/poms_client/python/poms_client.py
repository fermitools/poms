#!/usr/bin/env python

import urllib
import urllib2

def register_poms_campaign(campaign_name, user = None, experiment = None, version = None, dataset = None, campaign_definition = None, test = None)
    
    make_poms_call( 
	method = 'register_poms_campaign',
	campaign_name = campaign_name,
	user = user,
	experiment = experiment,
	version = version,
	dataset = datset,
	campaign_definition = campaign_definition,
	test = test)

def get_task_id(campaign, user = None, command_executed = None, input_dataset = None, parent_task_id = None, test = None)

    make_poms_call( 
         method = 'get_task_id',
	 campaign = campaign,
	 user = user,
	 command_executed = command_executed,
	 input_dataset = input_dataset,
	 parent_task_id = parent_task_id,
	 test = test)

def make_poms_call(**kwargs):

    method = kwargs.get("method")
    del kwargs["method"]

    if kwargs.get("test"):
        base='https://fermicloud045.fnal.gov/poms/'
        del kwargs["test"]
    else:
        base='https://pomsgpvm01.fnal.gov/poms/'

    c = urllib2.urlopen("%s/%s" % (base,method), urllib.urlencode(kwargs));
    print c.read()
  


if __name__ == '__main__':
    # simple tests...
