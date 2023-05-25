#!/bin/env python

import argparse
import requests
import os
import configparser
try:
    import requests.packages.urllib3 as urllib3
except ImportError:
    import urllib3

urllib3.disable_warnings()
config = configparser.ConfigParser()
config.read(os.environ["WEB_CONFIG"])

def parse_command_line():
    doc = "Retrives and displays POMS FERRY data of an experiment."
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('experiment', help="The experiment FERRY data will be retreived for.")
    parser.add_argument('-c', '--cert', help="Location of certificates - defaults to under ~/private/....")
    parser.add_argument('-s', '--skip_analysis', action="store_true", help='Do not include analysis users.')
    parser.add_argument('-f', '--ferry', help='FERRY url to use - defaults to %s.' % config.get("Ferry", "default_ferry_url"))

    args = parser.parse_args()
    return args

def query_ferry(cert, ferry_url, exp, role):
    results = {}
    url = ferry_url + "/getAffiliationMembersRoles?experimentname=%s&rolename=/%s/Role=%s" % (exp, exp, role)
    r = requests.get(url, verify=False, cert=(os.environ['X509_USER_CERT'], os.environ['X509_USER_KEY']))
    if r.status_code != requests.codes.get('ok'):
        print("get_ferry_experiment_users -- error status_code: %s  -- %s" % (r.status_code, url))
    else:
        results = r.json()
        ferry_error = results.get('ferry_error', None)
        if ferry_error is not None:
            print("get_ferry_experiment_users -- ferry_error: %s  URL: %s" % (str(ferry_error), url))
            results = {}
    return results.get(exp, {})

def query_superusers(cert, ferry_url, exp, anal_users):
    # Ferry does not support anyway to check for errors in result from this call. They aslo do
    # not provide the same data back that getAffiliationMemberRoles does.  Supposedly they will fix all this.
    results = []
    url = ferry_url + "/getSuperUserList?unitname=%s" % (exp)
    r = requests.get(url, verify=False, cert=(os.environ['X509_USER_CERT'], os.environ['X509_USER_KEY']))
    for row in r.json():
        uname = row.get('uname')
        su = {}
        for u in anal_users:
            if u['username'] == uname:
                su = u
                break
        results.append({'username': uname, 'commonname': su.get('commonname', 'not known'), 'role': 'superuser'})
    return results

def get_ferry_data(cert, ferry_url, exp, skip_analysis):
    """
    Query Ferry for exps users by Analysis and Production.  Join them in one dictionary.
      Dict Returned:
      {username: {'commonname':  'value',  'role': 'value'},
                 {'commonname':  'value',  'role': 'value'},
      }
    """
    anal_users = query_ferry(cert, ferry_url, exp, 'Analysis')
    prod_users = query_ferry(cert, ferry_url, exp, 'Production')
    su_users = query_superusers(cert, ferry_url, exp, anal_users)

    if skip_analysis is False:
        print("\n\nAnalysis:")
        for anal in anal_users:
            print("    username: %s\t\t  commonname: %s" % (anal.get('username'), anal.get('commonname')))

    print("\n\nProduction:")
    for prod in prod_users:
        print("    username: %s\t\t  commonname: %s" % (prod.get('username'), prod.get('commonname')))

    print("\n\nSuperusers:")
    for su in su_users:
        print("    username: %s\t\t  commonname: %s" % (su.get('username'), su.get('commonname')))

def main():
    args = parse_command_line()
    cert = args.cert or "/home/poms/private/gsi"
    ferry_url = args.ferry or config.get("Ferry", "default_ferry_url")
    get_ferry_data(cert, ferry_url, args.experiment, args.skip_analysis)

if __name__ == "__main__":
    main()
