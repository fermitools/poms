#!/usr/bin/env python

import datetime
import json
import logging
import os
import sys
import warnings

import requests

ZERO = datetime.timedelta(0)
class UTC(datetime.tzinfo):
    """UTC"""
    def utcoffset(self, dt):
        return ZERO
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return ZERO
utc = UTC()

try:
    import configparser as ConfigParser
except:
    import ConfigParser

rs = requests.Session()


def show_campaigns(test=None, **kwargs):
    # experiment=None, role = None, configfile=None, view_active=None, view_inactive=None,
    #                view_mine=None, view_others=None, view_production=None, update_view=None):
    """
    Return data about campaigns for the current experiment.
    """

    data, status = make_poms_call(
        method='show_campaigns',
        fmt='json',
        test_client=test,
        # configfile=configfile,
        # view_active=view_active,
        # view_inactive=view_inactive,
        # view_mine=view_mine,
        # view_others=view_others,
        # view_production=view_production,
        # update_view=update_view,
        **kwargs
    )
    # print("data = '{}'".format(data), file=open("output.txt", "w"))   # DEBUG
    return status in (200, 201), json.loads(data)


def show_campaign_stages(campaign_name=None, test=None, **kwargs):
                        #  experiment=None, role=None, configfile=None,
                        #  view_active=None, view_mine=None, view_others=None, view_production=None, update_view=None):
    """
    Return campaign stages for campaign for the current experiment.
    """
    data, status = make_poms_call(
        method='show_campaign_stages',
        fmt='json',
        test_client=test,
        campaign_name=campaign_name,
        # configfile=configfile,
        # view_active=view_active,
        # view_mine=view_mine,
        # view_others=view_others,
        # view_production=view_production,
        # update_view=update_view,
        **kwargs
    )
    return status in (200, 201), json.loads(data)

def update_submission(submission_id, jobsub_job_id=None, status=None, project=None, test=None, configfile=None, **kwargs):
    data, status = make_poms_call(
        method = 'update_submission',
        submission_id = submission_id,
        status=status,
        jobsub_job_id=jobsub_job_id,
        project=project,
        test_client=test,
        configfile=configfile,
        **kwargs)
    return status in (200, 201), data


def campaign_stage_submissions(experiment, role, campaign_name, stage_name, test=None, configfile=None, **kwargs):
    '''
    Return data about campaigns for the given experiment.
    '''
    data, status = make_poms_call(
        method='campaign_stage_submissions',
        fmt='json',
        experiment=experiment,
        role=role,
        campaign_name=campaign_name,
        stage_name=stage_name,
        test_client=test,
        configfile=configfile,
        **kwargs
    )
    return status in (200, 201), json.loads(data)


def submission_details(experiment, role, submission_id, test=None, configfile=None):
    '''
    return details about a certain submission
    '''
    data, status = make_poms_call(
        method='submission_details',
        experiment=experiment,
        role=role,
        submission_id=submission_id,
        fmt='json',
        test_client=test,
        configfile=configfile)

    return status in (200, 201), json.loads(data)


def upload_wf(file_name, test=None, experiment=None, configfile=None, replace=False, role = None):
    '''
    upload a campaign .ini file to the server, returns boolan OK flag, and json data from server
    '''
    data, status = make_poms_call(
        method='ini_to_campaign',
        files={'upload': (os.path.basename(file_name), open(file_name, 'rb'))},
        test_client=test,
        configfile=configfile,
        experiment=experiment,
        role=role,
        pcl_call=1,
        replace=replace
    )

    return status in (200, 201), json.loads(data)


def upload_file(file_name, test=None, experiment=None, role=None, configfile=None):
    '''
    upload a file to your $UPLOADS area on the poms server to be used in job launches.  returns boolean "Ok" value
    '''
    data, status = make_poms_call(
        method='upload_file',
        files={'filename': (os.path.basename(file_name), open(file_name, 'rb'))},
        test_client=test,
        experiment=experiment,
        role=role,
        configfile=configfile)
    return status == 303


def uploaded_files_rm(experiment, filename, test=None, role=None, configfile=None):
    """
    remove file(s) from your $UPLOADS area on the poms server.
    """
    data, status = make_poms_call(
        method='remove_uploaded_files',
        experiment=experiment,
        filename=filename,
        action='delete',
        redirect=0,
        test_client=test,
        role=role,
        configfile=configfile,
    )
    return data, status


def get_campaign_id(experiment, campaign_name, test=None, role=None, configfile=None):
    '''
    Get a campaign id by name. Returns integer campaign_id
    '''
    data, status = make_poms_call(
        method='get_campaign_id',
        experiment=experiment,
        campaign_name=campaign_name,
        role=role,
        test=test,
        configfile=configfile)
    return int(data)


def get_campaign_name(experiment, campaign_id, test=None, role=None, configfile=None):
    '''
    Get a campaign name by id. Returns campaign name
    '''
    data, status = make_poms_call(
        method='get_campaign_name',
        experiment=experiment,
        campaign_id=campaign_id,
        role=role,
        test=test,
        configfile=configfile)
    return data


def get_campaign_stage_id(experiment, campaign_name, campaign_stage_name, test=None, role=None, configfile=None):
    '''
    Get a campaign stage id by name. Returns stage id
    '''
    data, status = make_poms_call(
        method='get_campaign_stage_id',
        experiment=experiment,
        role=role,
        campaign_name=campaign_name,
        campaign_stage_name=campaign_stage_name,
        test=test,
        configfile=configfile)
    return int(data)


def get_campaign_stage_name(experiment, campaign_stage_id, test=None, role=None, configfile=None):
    '''
    Get a campaign stage name by id. Returns stage name
    '''
    data, status = make_poms_call(
        method='get_campaign_stage_name',
        experiment=experiment,
        role=role,
        campaign_stage_id=campaign_stage_id,
        test=test,
        configfile=configfile)
    return data

def update_stage_param_overrides(experiment, campaign_stage, param_overrides=None, test_param_overrides=None, test=None, role=None, configfile=None):
    """
    """
    data, status = make_poms_call(
        method="update_stage_param_overrides",
        experiment=experiment,
        role=role,
        campaign_stage=campaign_stage,
        param_overrides=param_overrides,
        test_param_overrides=test_param_overrides,
        test=test,
        configfile=configfile)
    return data


def register_poms_campaign(campaign_name, user=None, experiment=None, version=None, dataset=None, campaign_definition=None, test=None, role=None, configfile=None):
    '''
    deprecated: register campaign stage. returns "Campaign=<stage_id>"
    '''
    logging.warning("Notice: poms_client.register_poms_campaign() is deprecated")
    data, status = make_poms_call(
        method='register_poms_campaign',
        campaign_name=campaign_name,
        user=user,
        experiment=experiment,
        role=role,
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
                    task_id=None, test=None, experiment=None, role=None, configfile=None):

    logging.warning("Notice: poms_client.get_task_id_for() is deprecated, use get_submission_id")
    return get_submission_id_for(campaign, user, command_executed, input_dataset, parent_task_id, task_id, test, experiment, role, configfile)

def get_submission_id_for(campaign_stage_id, user=None, command_executed=None, input_dataset=None,
                          parent_submission_id=None, submission_id=None, test=None, experiment=None, role=None, configfile=None):
    '''
    get a submission id for a submission / or register the command
        executed for an existing submission.  Returns "Task=<submission_id>"
    '''
    logging.debug("in get_submission_id_for test = " + repr(test))
    data, status = make_poms_call(
        method='get_submission_id_for',
        campaign_stage_id=campaign_stage_id,
        user=user,
        command_executed=command_executed,
        input_dataset=input_dataset,
        parent_submission_id=parent_submission_id,
        submission_id=submission_id,
        experiment=experiment,
        role=role,
        test=test,
        configfile=configfile)
    data = data.replace('Task=', '')
    return int(data)


def launch_jobs(campaign, test=None, experiment=None, role=None, configfile=None):
    '''
    depecated: backward compatible call to launch jobs for a campaign stage
    '''
    logging.warning("Notice: poms_client.launch_jobs() is deprecated, use launch_campaign_stage_jobs")
    return launch_campaign_stage_jobs(campaign, test, experiment, role, configfile)[1] == 303


def launch_campaign_stage_jobs(campaign_stage_id, test=None, test_launch=None, experiment=None, role=None, configfile=None):
    '''
    launch jobs for a cammpaign stage: returns
    '''
    data, status = make_poms_call(
        method='launch_jobs',
        campaign_stage_id=campaign_stage_id,
        test_launch = test_launch,
        test=test,
        configfile=configfile,
        experiment=experiment,
        role=role)
    if status == 303:
        submission_id = int(data[data.rfind("_") + 1:])
    else:
        submission_id = None
    return data, status, submission_id

def modify_job_type_recoveries(job_type_id, recoveries, test=None, experiment=None, role=None, configfile=None):
    '''
    launch jobs for a cammpaign stage: returns
    '''

    # pass recoveries as a json dump if itsn't already
    if not isinstance(recoveries, str):
        recoveries = json.dumps(recoveries)

    data, status = make_poms_call(
        method='modify_job_type_recoveries',
        job_type_id=job_type_id,
        recoveries=recoveries,
        test=test,
        configfile=configfile,
        experiment=experiment,
        role=role)

    if data:
        data = json.loads(data)

    return data, status, job_type_id


def launch_campaign_jobs(campaign_id, test=None, test_launch=None, experiment=None, role=None, configfile=None):
    data, status = make_poms_call(
        method='launch_campaign',
        campaign_id=campaign_id,
        test_launch = test_launch,
        test=test,
        configfile=configfile,
        role=role,
        experiment=experiment)
    if status == 303:
        submission_id = int(data[data.rfind("_") + 1:])
    else:
        submission_id = None

    return data, status, submission_id


def job_type_rm(experiment, name, test=False, role=None, configfile=None):
    data, status = make_poms_call(
        method='job_type_rm',
        pcl_call=1,
        experiment=experiment,
        role=role,
        ae_definition_name=name,
        test=test,
        configfile=configfile)
    return data, status


def login_setup_rm(experiment, name, test=False, role=None, configfile=None):
    data, status = make_poms_call(
        method='login_setup_rm',
        pcl_call=1,
        experiment=experiment,
        role=role,
        ae_launch_name=name,
        test=test,
        configfile=configfile)
    return data, status


def launch_template_edit(action=None, launch_name=None, launch_host=None, user_account=None, launch_setup=None, experiment=None, pc_username=None, test_client=False, role=None, configfile=None):
    logging.debug("in get launch_jobs test_client = " + repr(test_client))
    method = 'launch_template_edit'
    ae_launch_name = launch_name
    ae_launch_host = launch_host
    ae_launch_account = user_account
    # ae_launch_setup = launch_setup
    if launch_setup is not None:
        ae_launch_setup = ""
        for arg_setup in launch_setup:
            ae_launch_setup = ae_launch_setup + str(arg_setup) + " "
        logging.debug("The ae_launch_setup is: " + str(ae_launch_setup))
    else:
        ae_launch_setup = launch_setup

    # pc_email = pc_email # no useing pc_username

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
                    role=role,
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
                    role=role,
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
                    role=role,

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


def campaign_definition_edit(output_file_patterns, launch_script, def_parameter=None, pc_username=None, action=None, name=None, experiment=None, recoveries=None,  test_client=False, role=None, configfile=None):
    logging.warning("Notice: poms_client.campaign_definition_edit() is deprecated, use job_type_edit")
    return job_type_edit(output_file_patterns, launch_script, def_parameter, pc_username,
                             action, name, experiment, recoveries, test_client, role, configfile)

def job_type_edit(output_file_patterns, launch_script, def_parameter=None, pc_username=None,
                             action=None, name=None, experiment=None, recoveries=None, test_client=False, role=None, configfile=None):
    # You can not modify the recovery_type from the poms_client (future feature)
    logging.debug("in get launch_jobs test_client = " + repr(test_client))
    method = "campaign_definition_edit"
    ae_definition_name = name
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
                                       role=role,

                                       ae_output_file_patterns=ae_output_file_patterns,
                                       ae_launch_script=ae_launch_script,
                                       ae_definition_parameters=ae_definition_parameters,
                                       ae_definition_recovery=ae_definition_recovery,
                                       test_client=test_client,
                                       configfile=configfile)
    return "status_code", status_code

    #return data['message']


def campaign_edit(**kwargs):
    logging.warning("Notice: campaign_edit has been replaced by campaign_stage_edit")
    raise DeprecationWarning("campaign_edit has been replaced by campaign_stage_edit")


def campaign_rm(experiment, name, test=False, role=None, configfile=None):
    data, status = make_poms_call(
        pcl_call=1,
        method='show_campaigns',
        action='delete',
        fmt='json',
        experiment=experiment,
        role=role,
        del_campaign_name=name,
        test=test,
        configfile=configfile)
    return data, status


def campaign_stage_edit(action, campaign_id, ae_stage_name, pc_username, experiment, vo_role,
                        dataset, ae_active, ae_split_type, ae_software_version,
                        ae_completion_type, ae_completion_pct, ae_param_overrides,
                        ae_depends, ae_launch_name, ae_campaign_definition, ae_test_param_overrides, test_client=None, role=None, configfile=None):
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
        logging.debug("The ae_param_overrides is: " + ae_param_overrides)
    else:
        logging.debug("conserving params, not override anything.")
    data, status_code = make_poms_call(pcl_call=1,
                                       method="campaign_stage_edit",
                                       action=action,
                                       ae_campaign_name=campaign_id,
                                       ae_stage_name=ae_stage_name,
                                       pc_username=pc_username,
                                       experiment=experiment,
                                       role=role,
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


def get_campaign_list(experiment=None, role=None, test_client=False):
    logging.debug("in get get_campaign_list  test_client = " + repr(test_client))
    res, sc = make_poms_call(method='campaign_list_json', experiment=experiment, role=role, test_client=test_client)
    d = {}
    logging.debug("got back: %s", res)
    for nid in json.loads(res):
        d[nid['name']] = nid['campaign_stage_id']
    return d


def tag_campaigns(tag, cids, experiment, role=None, test_client=False):
    logging.debug("in get get_campaign_list  test_client = " + repr(test_client))
    res, sc = make_poms_call(method='link_tags', campaign_id=cids, tag_name=tag, experiment=experiment, role=role, test_client=test_client)
    return sc == 200

global_role = None
global_experiment = None

def update_session_experiment(experiment, test_client=False):
    logging.debug("in update_session_experiment test_client = %s experiment %s " % (repr(test_client), experiment))
    global global_experiment
    global_experiment = experiment
    return True


def update_session_role(role, test_client=False):
    logging.debug("in update_session_role test_client = %s role %s" %(repr(test_client), role))
    global global_role
    global_role = role
    return True


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
    p0 = kwargs.get('configfile', '')
    p1 = os.path.dirname(sys.argv[0]) + '/client.cfg'
    p2 = os.environ.get('POMS_CLIENT_DIR', '') + '/bin/client.cfg'
    p3 = os.environ.get('POMS_CLIENT_DIR', '') + '/etc/client.cfg'
    for p in (p0, p1, p2, p3):
        if p and os.access(p, os.R_OK):
            cfgfile = p

    if kwargs.get('configfile', 'xx') != 'xx':
        del kwargs['configfile']

    config.read(cfgfile)
    _foundconfig = config
    return config

def base_path(test_client, config):

    if test_client:
        if test_client == "int":
            base = config.get('url', 'base_int_ssl')
        else:
            base = config.get('url', 'base_dev_ssl')

        logging.debug("base = " + base)
    else:
        #base=config['url']['base_prod']
        base = config.get('url', 'base_prod')
    return base

def check_stale_proxy(options):
    cert = auth_cert()
    rs.cert = (cert, cert)
    rs.verify = False
    try:
        url = "%s/file_uploads/%s/analysis/%s?fmt=json" % (
              base_path(options.test, getconfig({})), options.experiment, os.environ['USER'])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            c = rs.get(url)
        d = c.json()
        for f in d.get("file_stat_list", []):
            if f[0][:12] == "x509up_voms_":
                pdate = datetime.datetime.strptime(f[2], "%Y-%m-%dT%H:%M:%SZ")

                if options.verbose:
                    logging.info("proxy on POMS has date %sZ" % pdate)
                    logging.info("current time %sZ" % datetime.datetime.utcnow().isoformat())
                return datetime.datetime.utcnow() - pdate > datetime.timedelta(days=3)
    except Exception as e:
        logging.exception("Failed getting uploaded certificate date from POMS")
    # if we don't find it or something went wrong, its stale :-)
    return True

def make_poms_call(**kwargs):

    config = getconfig(kwargs)

    method = kwargs.get("method")
    del kwargs["method"]

    files = None
    if kwargs.get("files", None):
        files = kwargs["files"]
        del kwargs["files"]

    if kwargs.get("test", None) and not kwargs.get("test_client", None):
        kwargs["test_client"] = kwargs["test"]

    if kwargs.get("test", None):
        del kwargs["test"]

    if "experiment" not in kwargs or not kwargs["experiment"]:
        logging.debug("adding experiment %s" % global_experiment)
        kwargs["experiment"] = global_experiment

    if "role" not in kwargs or not kwargs["role"]:
        logging.debug("adding role %s" % global_role)
        kwargs["role"] = global_role

    test_client = kwargs.get("test_client", None)
    if "test_client" in kwargs:
        del kwargs["test_client"]

    logging.debug("in make_poms_call test_client = " + repr(test_client))

    base = base_path(test_client, config)

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
            res = res.replace("<br/>", "\n")
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
