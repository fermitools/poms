#!/usr/bin/env python

import urllib
import urllib2
import os

def register_poms_campaign(campaign_name, user = None, experiment = None, version = None, dataset = None, campaign_definition = None, test = None):
    return int(make_poms_call(
                    method = 'register_poms_campaign',
                    campaign_name = campaign_name,
                    user = user,
                    experiment = experiment,
                    version = version,
                    dataset = dataset,
                    campaign_definition = campaign_definition,
                    test = test).replace('Campaign=',''))

def get_task_id_for(campaign, user = None, command_executed = None, input_dataset = None, parent_task_id = None, test = None, experiment = None):

    return int(make_poms_call(
                    method = 'get_task_id_for',
                    campaign = campaign,
                    user = user,
                    command_executed = command_executed,
                    input_dataset = input_dataset,
                    parent_task_id = parent_task_id,
                    test = test).replace('Task=',''))


def launch_template_edit(action = None, name = None, launch_host = None, user_account = None, launch_setup = None, experiment = None, pc_email = None):


    method = 'launch_template_edit'
    action = action
    ae_launch_name = name
    ae_launch_host  = launch_host
    ae_launch_account = user_account
    ae_launch_setup = launch_setup
    experiment = experiment
    pc_email = pc_email

    if experiment == None or pc_email == None:
        print " You should provide an experiment name and email"
    else:

        if action == 'deleted':
            if ae_launch_name == None:
                print "For deleting you need to provide the name of the launch teamplate as name = name_of_your_launch_template"
            else:
                data = make_poms_call(
                    pcl_call=1,
                    method=method,
                    action = action,
                    ae_launch_name = ae_launch_name,
                    experiment = experiment)
                return data['message']

        if action == 'add':
            if ae_launch_name == None or ae_launch_host == None or ae_launch_account == None or ae_launch_setup == None:
                print "Your should provide the launch_name in order to add\
                        name, launch_host, user_account, launch_setup. \n\
                        Curently you provide name ="+ae_launch_name+",launch_host="+ae_launch_host+", user_account="+ae_launch_account+", launch_setup="+ae_launch_setup+"."
            else:
                data = make_poms_call(
                    pcl_call=1,
                    method = method,
                    action = action,
                    ae_launch_name = ae_launch_name,
                    experiment = experiment,

                    ae_launch_host = ae_launch_host,
                    ae_launch_account = ae_launch_account,
                    ae_launch_setup = ae_launch_setup)
                    ###The variables below are query in the CampaignsPOMS module
                    #ae_launch_id = ae_launch_id,
                    #experimenter_id = experimenter_id)

                return data['message']

        elif action == 'edit':
            if ae_launch_name == None:
                print "Your should provide the launch_name in order to edit\
                    name, launch_host, user_account, launch_setup. \n\
                    Curently you provide name = "+ae_launch_name
            else:
                data = make_poms_call(
                    pcl_call=1,
                    method = method,
                    action = action,
                    ae_launch_name = name,
                    experiment = experiment,

                    ae_launch_host = launch_host,
                    ae_launch_account = user_account,
                    ae_launch_setup = launch_setup)
                    ###The other var are query in the CampaignsPOMS module ae_launch_id, experimenter_id.
                return data['message']

        else:
            print "You should define an action on your launch_template, there are just \
            three posibilities: action = add, action = edit or action = remove. You choose action = "+action+"\n \
            You did not change anything in your template"


def campaign_definition_edit(input_files_per_job, output_files_per_job, output_file_patterns, launch_script, def_parameter, pc_email=None, action = None, name = None, experiment = None):
    # You can not modify the recovery_type from the poms_client (future feauture)
    method = "launch_template_edit"
    pc_email = pc_email
    action = action
    ae_definition_name = name
    experiment = experiment

    ae_input_files_per_job = input_files_per_job
    ae_output_files_per_job = output_files_per_job
    ae_output_file_patterns = output_file_patterns
    ae_launch_script = launch_script
    ae_definition_parameters= json.dumps(def_parameter)
    data = make_poms_call(  pcl_call=1,
                            method = method,
                            pc_email = pc_email,

                            action = action,
                            ae_definition_name = ae_definition_name,
                            experiment = experiment,

                            ae_input_files_per_job = ae_input_files_per_job ,
                            ae_output_files_per_job = ae_output_files_per_job,
                            ae_output_file_patterns = ae_output_file_patterns,
                            ae_launch_script = ae_launch_script,
                            ae_definition_parameters= ae_def_parameter
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
