#!/bin/env python

import argparse
import requests
try:
    import requests.packages.urllib3 as urllib3
except ImportError:
    import urllib3

urllib3.disable_warnings()

def parse_command_line():
    doc = "Retrives and displays POMS FERRY data of an experiment."
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('experiment', help="The experiment FERRY data will be retreived for.")
    parser.add_argument('-c', '--cert', help="Location of certificates - defaults to under ~/private/....")
    parser.add_argument('-s', '--skip_analysis', action="store_true", help='Do not include analysis users.')
    parser.add_argument('-f', '--ferry', help='FERRY url to use - defaults to https://ferry.fnal.gov:8443.')

    args = parser.parse_args()
    return args

def query_ferry(cert, ferry_url, exp, role):
    results = {}
    url = ferry_url + "/getAffiliationMembersRoles?experimentname=%s&rolename=/%s/Role=%s" % (exp, exp, role)
    r = requests.get(url, verify=False, cert=('%s/pomscert.pem' % cert, '%s/pomskey.pem' % cert))
    if r.status_code != requests.codes.get('ok'):
        print("get_ferry_experiment_users -- error status_code: %s  -- %s" % (r.status_code, url))
    else:
        results = r.json()
        ferry_error = results.get('ferry_error', None)
        if ferry_error is not None:
            print("get_ferry_experiment_users -- ferry_error: %s  URL: %s" % (str(ferry_error), url))
            results = {}
    return results.get(exp, {})

def get_ferry_data(cert, ferry_url, exp, skip_analysis):
    """
    Query Ferry for exps users by Analysis and Production.  Join them in one dictionary.
      Dict Returned:
      {username: {'commonname':  'value',  'role': 'value'},
                 {'commonname':  'value',  'role': 'value'},
      }
    """
    anal_users = {}
    cord_users = {}
    anal_users = query_ferry(cert, ferry_url, exp, 'Analysis')
    prod_users = query_ferry(cert, ferry_url, exp, 'Production')
    cord_users = query_ferry(cert, ferry_url, exp, 'superuser')

    if skip_analysis is False:
        print("\n\nAnalysis:")
        for anal in anal_users:
            print("    username: %s\t\t  commonname: %s" % (anal.get('username'), anal.get('commonname')))

    print("\n\nProduction:")
    for prod in prod_users:
        print("    username: %s\t\t  commonname: %s" % (prod.get('username'), prod.get('commonname')))

    print("\n\nsuperusers:")
    for cord in cord_users:
        print("    username: %s\t\t  commonname: %s" % (cord.get('username'), cord.get('commonname')))

def main():
    args = parse_command_line()
    cert = args.cert or "/home/poms/private/gsi"
    ferry_url = args.ferry or "https://ferry.fnal.gov:8443"
    get_ferry_data(cert, ferry_url, args.experiment, args.skip_analysis)

if __name__ == "__main__":
    main()
