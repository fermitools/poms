#!/usr/bin/env python

import requests
import os
import json

rs = requests.Session()

def register_poms_campaign(campaign_name, user = None, experiment = None, version = None, dataset = None, campaign_definition = None, test = None):
    data, status = make_poms_call(
                    method = 'register_poms_campaign',
                    campaign_name = campaign_name,
                    user = user,
                    experiment = experiment,
                    version = version,
                    dataset = dataset,
                    campaign_definition = campaign_definition,
                    test = test).replace('Campaign=','')
    return int(data)

def get_task_id_for(campaign, user = None, command_executed = None, input_dataset = None, parent_task_id = None, test = None, experiment = None):

    data, status = make_poms_call(
                    method = 'get_task_id_for',
                    campaign = campaign,
                    user = user,
                    command_executed = command_executed,
                    input_dataset = input_dataset,
                    parent_task_id = parent_task_id,
                    test = test).replace('Task=','')
    return int(data)


def launch_template_edit(action = None, name = None, launch_host = None, user_account = None, launch_setup = None, experiment = None, pc_username = None, test_client=False):


    method = 'launch_template_edit'
    action = action
    ae_launch_name = name
    ae_launch_host  = launch_host
    ae_launch_account = user_account
    ae_launch_setup = launch_setup
    experiment = experiment
    #pc_email = pc_email #no useing pc_username

    if experiment == None or pc_username == None:
        print " You should provide an experiment name and email"
    else:

        if action == 'delete':
            if ae_launch_name == None:
                print "For deleting you need to provide the name of the launch teamplate as name = name_of_your_launch_template"
            else:
                data, status_code = make_poms_call(
                    pcl_call=1,
                    pc_username=pc_username,
                    method=method,
                    action = action,
                    ae_launch_name = ae_launch_name,
                    experiment = experiment,
                    test_client=test_client)
                return "status_code", status_code

        if action == 'add':
            if ae_launch_name == None or ae_launch_host == None or ae_launch_account == None or ae_launch_setup == None:
                print "Your should provide the launch_name in order to add name, launch_host, user_account, launch_setup. \n\
                        Curently you provide name ="+str(ae_launch_name)+",launch_host="+str(ae_launch_host)+", user_account="+str(ae_launch_account)+", launch_setup="+str(ae_launch_setup)+"."
            else:
                data, status_code  = make_poms_call(
                    pcl_call=1,
                    pc_username=pc_username,
                    method = method,
                    action = action,
                    name = ae_launch_name,
                    experiment = experiment,

                    ae_launch_host = ae_launch_host,
                    ae_launch_account = ae_launch_account,
                    ae_launch_setup = ae_launch_setup,
                    test_client=test_client)
                    ###The variables below are query in the CampaignsPOMS module
                    #ae_launch_id = ae_launch_id,
                    #experimenter_id = experimenter_id)
                return 'status_code', status_code


        elif action == 'edit':
            if ae_launch_name == None:
                print "Your should provide the launch_name in order to edit\n\
                    Curently you provide name = "+ae_launch_name
            else:
                data, status_code = make_poms_call(
                    pcl_call=1,
                    pc_username=pc_username,
                    method = method,
                    action = action,
                    ae_launch_name = ae_launch_name,
                    experiment = experiment,

                    ae_launch_host = launch_host,
                    ae_launch_account = user_account,
                    ae_launch_setup = launch_setup,
                    test_client=test_client)
                return "status_code=", status_code
                    ###The other var are query in the CampaignsPOMS module ae_launch_id, experimenter_id.
                #return data['message']

        else:
            print "You should define an action on your launch_template, there are just \
            three posibilities: action = add, action = edit or action = delete. You choose action = "+action+"\n \
            You did not change anything in your template"


def campaign_definition_edit(output_file_patterns, launch_script,
                            def_parameter, pc_username=None, action = None, name = None, experiment = None, test_client=False):
    # You can not modify the recovery_type from the poms_client (future feauture)
    test_client = test_client
    method = "campaign_definition_edit"
    pc_username = pc_username
    action = action
    ae_definition_name = name
    experiment = experiment

    ae_output_file_patterns = output_file_patterns
    ae_launch_script = launch_script
    ae_definition_parameters= json.dumps(def_parameter)
    data, status_code = make_poms_call(pcl_call=1,
                            method = method,
                            pc_username = pc_username,

                            action = action,
                            ae_definition_name = ae_definition_name,
                            experiment = experiment,

                            ae_output_file_patterns = ae_output_file_patterns,
                            ae_launch_script = ae_launch_script,
                            ae_definition_parameters= ae_definition_parameters,
                            test_client=test_client
                            )
    return "status_code =", status_code

    #return data['message']


def campaign_edit (action, ae_campaign_name, pc_username, experiment, vo_role,
                    dataset, ae_active, ae_split_type, ae_software_version,
                    ae_completion_type, ae_completion_pct, ae_param_overrides,
                    ae_depends, ae_launch_name, ae_campaign_definition, test_client):
    method="campaign_edit"
    data, status_code = make_poms_call(pcl_call=1,
                            method=method,
                            action=action,
                            ae_campaign_name=ae_campaign_name,
                            pc_username=pc_username,
                            experiment=experiment,
                            ae_vo_role=vo_role,
                            ae_dataset=dataset,
                            ae_active=ae_active,
                            ae_split_type=ae_split_type,
                            ae_software_version=ae_software_version,
                            ae_completion_type=ae_completion_type,
                            ae_completion_pct=ae_completion_pct,
                            ae_param_overrides=ae_param_overrides,
                            ae_depends=ae_depends,
                            ae_launch_name=ae_launch_name,
                            ae_campaign_definition=ae_campaign_definition,
                            test_client=test_client)
    return "status_code =", status_code
    #return data['message']


def auth_cert():
        #rs.cert = '/tmp/x509up_u`id -u`'
        cert=os.environ.get('X509_USER_PROXY')
        '''
        if not cert:
            cert=os.environ.get('X509_USER_CERT')
            key=os.environ.get('X509_USER_KEY')
            if cert and key: cert =(cert,key)
        '''
        if not cert:
            #proxypath = '/tmp/x509up_u%d' % os.getuid()
            proxypath = "/tmp/x509up_u50765"
            if os.path.exists(proxypath):
                cert=proxypath
        if not cert:
            print "You should generate a proxy for use the client, you can use kx509 to generate your proxy. If you have a proxy please provide the location at the enviroment variable X509_USER_PROXY"
        return cert

def make_poms_call(**kwargs):
    method = kwargs.get("method")
    del kwargs["method"]
    test_client=kwargs.get("test_client")

    if kwargs.get("test"):
        base='http://fermicloud045.fnal.gov:8080/poms/'
        del kwargs["test"]
    elif test_client:
        #base='http://pomsgpvm01.fnal.gov:8080/poms/'
        #base='http://localhost:8888/poms/'
        base='https://fermicloud045.fnal.gov:8443/poms/'
    else:
        base='http://pomsgpvm01.fnal.gov:8080/poms/'


    for k in kwargs.keys():
        if kwargs[k] == None:
            del kwargs[k]
    if os.environ.get("POMS_CLIENT_DEBUG", None):
        print "poms_client: making call %s( %s ) at %s with the proxypath = %s" % (method, kwargs, base, cert)
    cert=auth_cert()
    rs.cert=cert
    rs.key=cert
    print "poms_client: making call %s( %s ) at %s with the proxypath = %s" % (method, kwargs, base, cert)
    c = rs.post("%s/%s" % (base,method), data=kwargs, verify=False);
    res = c.text
    status_code = c.status_code
    c.close()
    print "\n\nres =", res
    print "status_code", status_code
    return res, status_code


if __name__ == '__main__':
    # simple tests...
    res = make_poms_call(test=True, method="active_jobs")
    tid = get_task_id_for(test = True, campaign=14, command_executed="fake test job")
    cid = register_poms_campaign("mwm_client_test",  user = "mengel", experiment = "samdev", version = "v0_0", dataset = "mwm_test_data", test = True)
    print "got task id ", tid
    print "got campaign id ", cid
