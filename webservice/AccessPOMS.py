#!/usr/bin/env python
### This module contain the methods in order to do the control access in POMS
### List of methods:  can_report_data, can_db_admin
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
### December, 2016.

class AccessPOMS():


    def can_report_data(self, gethead, loghandle, seshandle ):
        xff = gethead('X-Forwarded-For', None)
        ra =  gethead('Remote-Addr', None)
        user = gethead('X-Shib-Userid', None)
        loghandle("can_report_data: Remote-addr: %s" %  ra)
        if ra.startswith('131.225.67.'):
            return 1
        if ra.startswith('131.225.80.'):
            return 1
        if ra == '127.0.0.1' and xff and xff.startswith('131.225.67'):
             # case for fifelog agent..
             return 1
        if ra != '127.0.0.1' and xff and xff.startswith('131.225.80'):
             # case for jobsub_q agent (currently on bel-kwinith...)
             return 1
        if ra == '127.0.0.1' and xff == None:
             # case for local agents
             return 1
        if (seshandle('experimenter')).is_root():
             # special admins
             return 1
        return 0


    def can_db_admin(self, gethead, seshandle):
        xff = gethead('X-Forwarded-For', None)
        ra =  gethead('Remote-Addr', None)
        user = gethead('X-Shib-Userid', None)
        if ra in ['127.0.0.1','131.225.80.97'] and xff == None:
             # case for local agents
             return 1
        if (seshandle('experimenter')).is_root():
             # special admins
             return 1
        return 0
