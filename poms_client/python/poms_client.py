#!/usr/bin/env python

import json
import logging
import os
import sys
import warnings

import requests

try:
    import configparser as ConfigParser
except:
    import ConfigParser

rs = requests.Session()


def show_campaigns(test=None, experiment=None, configfile=None, view_active=None, view_mine=None, view_others=None, view_production=None, update_view=None):
    '''
    Return data about campaigns for the current experiment.
    '''

    data, status = make_poms_call(
        method='show_campaigns',
        format='json',
        test_client=test,
        configfile=configfile,
        view_active=view_active,
        view_mine=view_mine,
        view_others=view_others,
        view_production=view_production,
        update_view=update_view
    )
    return status in (200, 201), json.loads(data)


def show_campaign_stages(campaign_name, test=None, experiment=None, configfile=None,
                         view_active=None, view_mine=None, view_others=None, view_production=None, update_view=None):
    '''
    Return campaign stages for campaign for the current experiment.
    '''
    data, status = make_poms_call(
        method='show_campaign_stages',
        format='json',
        campaign_name=campaign_name,
        test_client=test,
        configfile=configfile,
        view_active=view_active,
        view_mine=view_mine,
        view_others=view_others,
        view_production=view_production,
        update_view=update_view
    )
    return status in (200, 201), json.loads(data)


def campaign_stage_submissions(campaign_id, campaign_name, stage_name, campaign_stage_id=None, test=None, experiment=None, configfile=None):
    '''
    Return data about campaigns for the current experiment.
    '''

    data, status = make_poms_call(
        method='campaign_stage_submissions',
        format='json',
        test_client=test,
        configfile=configfile,
        campaign_id=campaign_id,
        campaign_stage_id=campaign_stage_id,
        campaign_name=campaign_name,
        stage_name=stage_name,
    )
    return status in (200, 201), json.loads(data)


def submission_details(submission_id, test=None, experiment=None, configfile=None):
    '''
    return details about a certain submission
    '''

    data, status = make_poms_call(
        method='submission_details',
        submission_id=submission_id,
        format='json',
        test_client=test,
        configfile=configfile)

    return status in (200, 201), json.loads(data)


def upload_wf(file_name, test=None, experiment=None, configfile=None):
    '''
    upload a campaign .ini file to the server, returns boolan OK flag, and json data from server
    '''
    data, status = make_poms_call(
        method='ini_to_campaign',
        files={'upload': (os.path.basename(file_name), open(file_name, 'rb'))},
        test_client=test,
        configfile=configfile)

    return status in (200, 201), json.loads(data)


def upload_file(file_name, test=None, experiment=None, configfile=None):
    '''
    upload a file to your $UPLOADS area on the poms server to be used in jo b launches.  returns boolean "Ok" value
    '''
    data, status = make_poms_call(
        method='upload_file',
        files={'filename': (os.path.basename(file_name), open(file_name, 'rb'))},
        test_client=test,
        configfile=configfile)

    return status == 303


def get_campaign_id(campaign_name, test=None, configfile=None):
    '''
    deprecated: get a campaign stage id by name. Returns integer campaign_stage_id
    '''
    data, status = make_poms_call(
        method='get_campaign_id',
        campaign_name=campaign_name,
        test=test,
        configfile=configfile)
    return int(data)


def register_poms_campaign(campaign_name, user=None, experiment=None, version=None, dataset=None, campaign_definition=None, test=None, configfile=None):
    '''
    deprecated: register campaign stage. returns "Campaign=<stage_id>"
    '''
    data, status = make_poms_call(
        method='register_poms_campaign',
        campaign_name=campaign_name,
        user=user,
        experiment=experiment,
        version=version,
        dataset=dataset,
        campaign_definition=campaign_definition,
        test=test,
        configfile=configfile)
    logging.debug("in register_poms_campaign test = " + repr(test))
    print("got data: %s" % data)
    data = data.replace('Campaign=', '')
    return int(data)


def get_task_id_for(campaign, user=None, command_executed=None, input_dataset=None, parent_task_id=None,
                    task_id=None, test=None, experiment=None, configfile=None):
    '''
    get a submission id for a submission / or register the command
        executed for an existing submission.  Returns "Task=<submission_id>"
    '''
    logging.debug("in get task_id_for test = " + repr(test))
    data, status = make_poms_call(
        method='get_task_id_for',
        campaign=campaign,
        user=user,
        command_executed=command_executed,
        input_dataset=input_dataset,
        parent_task_id=parent_task_id,
        task_id=task_id,
        test=test,
        configfile=configfile)
    data = data.replace('Task=', '')
    return int(data)


def launch_jobs(campaign, test=None, experiment=None, configfile=None):
    '''
    depecated: backward compatible call to launch jobs for a campaign stage
    '''
    return launch_campaign_stage_jobs(campaign, test, experiment, configfile)[1] == 303


def launch_campaign_stage_jobs(campaign_stage_id, test=None, experiment=None, configfile=None):
    '''
    launch jobs for a cammpaign stage: returns
    '''
    data, status = make_poms_call(
        method='launch_jobs',
        campaign_stage_id=campaign_stage_id,
        test=test,
        configfile=configfile,
        experiment=experiment)
    if status == 303:
        submission_id = int(data[data.rfind("_") + 1:])
    else:
        submission_id = None
    return data, status, submission_id


def launch_campaign_jobs(campaign_id, test=None, experiment=None, configfile=None):
    data, status = make_poms_call(
        method='launch_campaign',
        campaign_id=campaign_id,
        test=test,
        configfile=configfile,
        experiment=experiment)
    if status == 303:
        submission_id = int(data[data.rfind("_") + 1:])
    else:
        submission_id = None

    return data, status, submission_id


def launch_template_edit(action=None, launch_name=None, launch_host=None, user_account=None, launch_setup=None,
                         experiment=None, pc_username=None, test_client=False, configfile=None):
    logging.debug("in get launch_jobs test_client = " + repr(test_client))
    method = 'launch_template_edit'
    action = action
    ae_launch_name = launch_name
    ae_launch_host = launch_host
    ae_launch_account = user_account
    experiment = experiment
    # ae_launch_setup = launch_setup
    if launch_setup is not None:
        ae_launch_setup = ""
        for arg_setup in launch_setup:
            ae_launch_setup = ae_launch_setup + str(arg_setup) + " "
        logging.debug("The ae_launch_setup is: " + str(ae_launch_setup))
    else:
        ae_launch_setup = launch_setup

    #pc_email = pc_email #no useing pc_username

    if experiment is None or pc_username is None:
        logging.error(" You should provide an experiment name and email")
    else:

        if action == 'delete':
            if ae_launch_name is None:
                logging.error("For deleting you need to provide the name of the launch template as name = name_of_your_launch_template")
            else:
                data, status_code = make_poms_call(
                    pcl_call=1,
                    pc_username=pc_username,
                    method=method,
                    action=action,
                    ae_launch_name=ae_launch_name,
                    ae_launch_host=ae_launch_host,
                    ae_launch_account=ae_launch_account,
                    ae_launch_setup=ae_launch_setup,
                    experiment=experiment,
                    test_client=test_client,
                    configfile=configfile)
                return "status_code", status_code

        if action == 'add':
            if ae_launch_name is None or ae_launch_host is None or ae_launch_account is None or ae_launch_setup is None:
                logging.error("Your should provide the launch_name in order to add name, launch_host, user_account, launch_setup. \n\
                        Currently you provide launch_name=" + str(ae_launch_name) + ",launch_host=" + str(ae_launch_host) +
                              ", user_account=" + str(ae_launch_account) + ", setup=" + str(ae_launch_setup) + ".")
            else:
                data, status_code = make_poms_call(
                    pcl_call=1,
                    pc_username=pc_username,
                    method=method,
                    action=action,
                    ae_launch_name=ae_launch_name,
                    experiment=experiment,
                    ae_launch_host=ae_launch_host,
                    ae_launch_account=ae_launch_account,
                    ae_launch_setup=ae_launch_setup,
                    test_client=test_client,
                    configfile=configfile)
                    ###The variables below are query in the CampaignsPOMS module
                    #ae_launch_id = ae_launch_id,
                    #experimenter_id = experimenter_id)
                return 'status_code', status_code


        elif action == 'edit':
            if ae_launch_name is None:
                logging.error("Your should provide the launch_name in order to edit\n\
                    Currently you provide name = " + str(ae_launch_name))
            else:
                data, status_code = make_poms_call(
                    pcl_call=1,
                    pc_username=pc_username,
                    method=method,
                    action=action,
                    ae_launch_name=ae_launch_name,
                    experiment=experiment,

                    ae_launch_host=launch_host,
                    ae_launch_account=user_account,
                    ae_launch_setup=launch_setup,
                    test_client=test_client,
                    configfile=configfile)
                return "status_code", status_code
                    ###The other var are query in the CampaignsPOMS module ae_launch_id, experimenter_id.
                #return data['message']

        else:
            logging.error("You should define an action on your launch_template, there are just \
            three posibilities: action = add, action = edit or action = delete. You choose action = "+action+"\n \
            You did not change anything in your template")

        return "failed", -1


def campaign_definition_edit(output_file_patterns, launch_script, def_parameter=None, pc_username=None,
                             action=None, name=None, experiment=None, recoveries=None, test_client=False, configfile=None):
    # You can not modify the recovery_type from the poms_client (future feature)
    logging.debug("in get launch_jobs test_client = " + repr(test_client))
    method = "campaign_definition_edit"
    # pc_username = pc_username
    # action = action
    ae_definition_name = name
    # experiment = experiment
    ae_definition_recovery = recoveries
    ae_output_file_patterns = output_file_patterns
    ae_launch_script = launch_script
    if launch_script is not None:
        ae_launch_script = ""
        for arg_setup in launch_script:
            ae_launch_script = ae_launch_script + str(arg_setup) + " "
        logging.info("The ae_launch_setup is: " + str(ae_launch_script))
    else:
        ae_launch_script = launch_script

    if isinstance(def_parameter, str):
        try:
            def_parameter = json.loads(def_parameter)
        except:
            logging.error("please use JSON format for the parameter overrides")
            raise

    ae_definition_parameters = json.dumps(def_parameter)
    data, status_code = make_poms_call(pcl_call=1,
                                       method=method,
                                       pc_username=pc_username,

                                       action=action,
                                       ae_definition_name=ae_definition_name,
                                       experiment=experiment,

                                       ae_output_file_patterns=ae_output_file_patterns,
                                       ae_launch_script=ae_launch_script,
                                       ae_definition_parameters=ae_definition_parameters,
                                       ae_definition_recovery = ae_definition_recovery,
                                       test_client=test_client,
                                       configfile=configfile)
    return "status_code", status_code

    #return data['message']

def campaign_edit(**kwargs):
    print("campaign_edit has been replaced by campaign_stage_edit")


def campaign_stage_edit(action, campaign_id, ae_stage_name, pc_username, experiment, vo_role,
                        dataset, ae_active, ae_split_type, ae_software_version,
                        ae_completion_type, ae_completion_pct, ae_param_overrides,
                        ae_depends, ae_launch_name, ae_campaign_definition, ae_test_param_overrides, test_client=None, configfile=None):
    logging.debug("in get campaign_stage_edit test_client = " + repr(test_client))
    method = "campaign_stage_edit"
    logging.debug("#" * 10)
    logging.debug(ae_param_overrides)

    # if already packed as a string, unpack it so we can repack it...
    if isinstance(ae_param_overrides, str):
        try:
            ae_param_overrides = json.loads(ae_param_overrides)
        except:
            logging.error("please use JSON format for the parameter overrides")
            raise

    if ae_param_overrides:
        try:
            ae_param_overrides = json.dumps(ae_param_overrides)
        except:
            logging.error("please use data dumpable as JSON for the parameter overrides")
            raise
        logging.debug("#"*10)
        logging.debug("type" + str(type(ae_param_overrides)))
        logging.debug("The ae_param_overrides is: " +  ae_param_overrides)
    else:
        logging.debug("conserving params, not override anything.")
    data, status_code = make_poms_call(pcl_call=1,
                                       method="campaign_stage_edit",
                                       action=action,
                                       ae_campaign_name=campaign_id,
                                       ae_stage_name=ae_stage_name,
                                       pc_username=pc_username,
                                       experiment=experiment,
                                       ae_vo_role=vo_role,
                                       ae_dataset=dataset,
                                       # ae_active=ae_active,
                                       ae_split_type=ae_split_type,
                                       ae_software_version=ae_software_version,
                                       ae_completion_type=ae_completion_type,
                                       ae_completion_pct=ae_completion_pct,
                                       ae_param_overrides=ae_param_overrides,
                                       ae_test_param_overrides=ae_param_overrides,
                                       ae_depends=ae_depends,
                                       ae_launch_name=ae_launch_name,
                                       ae_campaign_definition=ae_campaign_definition,
                                       test_client=test_client,
                                       configfile=configfile)
    return "status_code", status_code
    #return data['message']


def get_campaign_list(test_client=False):
    logging.debug("in get get_campaign_list  test_client = " + repr(test_client))
    res, sc = make_poms_call(method='campaign_list_json', test_client=test_client)
    d = {}
    logging.debug("got back: %s", res)
    for nid in json.loads(res):
        d[nid['name']] = nid['campaign_stage_id']
    return d


def tag_campaigns(tag, cids, experiment, test_client=False):
    logging.debug("in get get_campaign_list  test_client = " + repr(test_client))
    res, sc = make_poms_call(method='link_tags', campaign_id=cids, tag_name=tag, experiment=experiment, test_client=test_client)
    return sc == 200


def update_session_experiment(experiment, test_client=False):
    logging.debug("in update_session_experiment test_client = " + repr(test_client))
    res, sc = make_poms_call(method='update_session_experiment', session_experiment = experiment, test_client=test_client)
    return sc == 200

def update_session_role(role, test_client=False):
    logging.debug("in update_session_role test_client = " + repr(test_client))
    res, sc = make_poms_call(method='update_session_role', session_role=role, test_client=test_client)
    return sc == 200

def auth_cert():
    # rs.cert = '/tmp/x509up_u`id -u`'
    cert = os.environ.get('X509_USER_PROXY', None)
    if not cert:
        proxypath = '/tmp/x509up_u%d' % os.getuid()
        # proxypath = "/tmp/x509up_u50765"
        if os.path.exists(proxypath):
            cert = proxypath
    if not cert:
        os.system('kx509')
        proxypath = '/tmp/x509up_u%d' % os.getuid()
        # proxypath = "/tmp/x509up_u50765"
        if os.path.exists(proxypath):
            cert = proxypath
        if not cert:
            logging.error("Unable to find a client certificate, or to generate one with kx509.  "
                          "You should generate a proxy for use the client, you can use kx509 to generate your proxy. "
                          "If you have a proxy please provide the location at the enviroment variable X509_USER_PROXY")
    return cert

_foundconfig = None
def getconfig(kwargs):
    global _foundconfig
    if _foundconfig:
        return _foundconfig
    config = ConfigParser.ConfigParser()
    p0 = kwargs.get('configfile','')
    p1 = os.path.dirname(sys.argv[0])+'/client.cfg'
    p2 = os.environ.get('POMS_CLIENT_DIR','') +'/bin/client.cfg'
    p3 = os.environ.get('POMS_CLIENT_DIR','') +'/etc/client.cfg'
    for p in (p0, p1, p2, p3):
        if p and os.access(p,os.R_OK):
            cfgfile = p

    if kwargs.get('configfile', 'xx') != 'xx':
        del kwargs['configfile']

    config.read(cfgfile)
    _foundconfig = config
    return config


def make_poms_call(**kwargs):

    config = getconfig(kwargs)


    method = kwargs.get("method")
    del kwargs["method"]

    files = None
    if kwargs.get("files",None):
        files=kwargs["files"]
        del kwargs["files"]

    if kwargs.get("test", None) and not kwargs.get("test_client",None):
        kwargs["test_client"] = kwargs["test"]

    if kwargs.get("test", None):
        del kwargs["test"]


    test_client = kwargs.get("test_client", None)
    if kwargs.has_key("test_client"):
        del kwargs["test_client"]

    logging.debug("in make_poms_call test_client = " + repr(test_client))

    if test_client:
        if test_client == "int":
            base=config.get('url','base_int_ssl')
        else:
            base=config.get('url','base_dev_ssl')

        logging.debug("base = " + base)
    else:
        #base=config['url']['base_prod']
        base = config.get('url', 'base_prod')

    for k in list(kwargs.keys()):
        if kwargs[k] is None:
            del kwargs[k]

    cert = auth_cert()
    if os.environ.get("POMS_CLIENT_DEBUG", None):
        logging.debug("poms_client: making call %s( %s ) at %s with the proxypath = %s" % (method, kwargs, base, cert))
    if cert is None and base[:6] == "https:":
        return ("No client certificate", 500)
    rs.cert = (cert, cert)
    rs.verify = False
    logging.debug("poms_client: making call %s( %s ) at %s with the proxypath = %s" % (method, kwargs, base, cert))

    # ignore insecure request warnings...
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        c = rs.post("%s/%s" % (base, method), data=kwargs, files=files, verify=False, allow_redirects=False)
    res = c.text
    status_code = c.status_code
    c.close()
    #logging.debug("\n\nres =" + str(res))
    logging.debug("status_code: " + str(status_code))
    if status_code == 303:
        res = c.headers['Location']
        # res = base + res[res.find('/poms/'):]
    elif status_code not in (200, 202):
        if res.find("Traceback"):
            res = res[res.find("Traceback"):]
            res = res.replace("<br/>","\n")
            res = res[:res.find("</label>")]
            logging.debug("Server: " + res)
        else:
            logging.debug("Error text" + res)
        raise RuntimeError("POMS call %s error: HTTP status: %d\n%s" % (method, status_code, res))
    return res, status_code


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # simple tests...
    res, sc = make_poms_call(test=True, method="active_jobs")
    tid = get_task_id_for(test=True, campaign=14, command_executed="fake test job")
    cid = register_poms_campaign("mwm_client_test", user="mengel", experiment="samdev", version="v0_0", dataset="mwm_test_data", test=True)
    logging.info("got task id " + str(tid))
    logging.info("got campaign id " + str(cid))
