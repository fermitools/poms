#!/usr/bin/env python


# This module contain the methods that handle the Calendar. (5 methods)
# List of methods: handle_dates, quick_search, jump_to_job,
# Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
# written by Marc Mengel, Stephen White and Michael Gueith.
### October, 2016.
from datetime import datetime, timedelta
from .utc import utc
from .poms_model import Experimenter, ExperimentsExperimenters
import base64
import tempfile
#import ctypes
import sys
import requests
import secrets
import subprocess
import json
import stat
import time
import os
from . import logit
import traceback
#import traceback
#from ctypes import *
#import gssapi

class UtilsPOMS:
    # h3. __init__
    def __init__(self, ps):
        self.poms_service = ps

    # this method was deleted from the main script
    # h3. handle_dates
    def handle_dates(self, ctx, baseurl):
        """
        tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange = self.handle_dates(tmax, tdays, name)
        assuming tmin, tmax, are date strings or None, and tdays is
        an integer width in days, come up with real datetimes for
        tmin, tmax, and string versions, and next ane previous links
        and a string describing the date range.  Use everywhere.
        """

        tmin = ctx.tmin
        tmax = ctx.tmax
        tdays = ctx.tdays
        # if they set max and min (i.e. from calendar) set tdays from that.
        if not tmax in (None, "") and not tmin in (None, ""):
            if isinstance(tmin, str):
                tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
            if isinstance(tmax, str):
                tmax = datetime.strptime(tmax[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
            tdays = (tmax - tmin).total_seconds() / 86400.0

        if tmax in (None, ""):
            if tmin not in (None, "") and tdays not in (None, ""):
                if isinstance(tmin, str):
                    tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
                tmax = tmin + timedelta(days=float(tdays))
            else:
                # if we're not given a max, pick now
                tmax = datetime.now(utc)

        elif isinstance(tmax, str):
            tmax = datetime.strptime(tmax[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)

        if tdays in (None, ""):  # default to one day
            tdays = 1

        tdays = float(tdays)

        if tmin in (None, ""):
            tmin = tmax - timedelta(days=tdays)

        elif isinstance(tmin, str):
            tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)

        tsprev = tmin.strftime("%Y-%m-%d+%H:%M:%S")
        tsnext = (tmax + timedelta(days=tdays)).strftime("%Y-%m-%d+%H:%M:%S")
        tmax_s = tmax.strftime("%Y-%m-%d %H:%M:%S")
        tmin_s = tmin.strftime("%Y-%m-%d %H:%M:%S")
        prevlink = "%s/%stmax=%s&tdays=%d" % (self.poms_service.path, baseurl, tsprev, tdays)
        nextlink = "%s/%stmax=%s&tdays=%d" % (self.poms_service.path, baseurl, tsnext, tdays)
        # if we want to handle hours / weeks nicely, we should do
        # it here.
        plural = "s" if tdays > 1.0 else ""
        trange = '%6.1f day%s ending <span class="tmax">%s</span>' % (tdays, plural, tmax_s)

        # redundant, but trying to rule out tz woes here...
        tmin = tmin.replace(tzinfo=utc)
        tmax = tmax.replace(tzinfo=utc)
        tdays = int((tmax - tmin).total_seconds() / 864.0) / 100

        return (tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange, tdays)

    # h3. quick_search
    def quick_search(self, ctx, search_term):
        search_term = search_term.strip()
        search_term = search_term.replace("*", "%")
        raise ctx.HTTPRedirect("%s/search_campaigns?search_term=%s" % (self.poms_service.path, search_term))

    # h3. get
    def getSavedExperimentRole(self, ctx):
        experiment, role = (
            ctx.db.query(Experimenter.session_experiment, Experimenter.session_role)
            .filter(Experimenter.username == ctx.username)
            .first()
        )
        return experiment, role

    # h3. update_session_experiment
    def update_session_experiment(self, ctx, session_experiment, **kwargs):
        # check for switching to an experiment where we can't have our
        # current role...
        exp = ctx.get_experimenter()
        exex = (
            ctx.db.query(ExperimentsExperimenters)
            .filter(
                ExperimentsExperimenters.experiment == session_experiment,
                ExperimentsExperimenters.experimenter_id == exp.experimenter_id,
            )
            .first()
        )

        if not exp.root:
            if not exex:
                raise PermissionError("Cannot change: not a member of experiment %s" % session_experiment)

            if exex.role == "analysis":
                ctx.role = "analysis"

            if exex.role == "production" and ctx.role == "superuser":
                ctx.role = "production"

        ctx.experiment = session_experiment

        fields = {"session_experiment": session_experiment, "session_role": ctx.role}
        ctx.db.query(Experimenter).filter(Experimenter.username == ctx.username).update(fields)

        ctx.db.commit()

    # h3. update_session_role
    def update_session_role(self, ctx, session_role, **kwargs):

        ctx.db.query(Experimenter).filter(Experimenter.username == ctx.username).update({"session_role": session_role})

        ctx.db.commit()
    # Leaving debug on for the time being to find errors if they occur
    def get_oidc_url(self, ctx, vaultserver = None, oidcpath = None, referer = None, debug = True): 
        
        if not vaultserver:
            vaultserver = ctx.web_config.get("tokens","vaultserverfull")
        if not oidcpath:
            oidcpath = ctx.web_config.get("tokens","oidcpath")

        logit.log("Attempting OIDC authentication")
        role = "default"
        
        issuer = ctx.experiment
        if issuer == "samdev":
            issuer = "fermilab"
        oidcpath = oidcpath.replace("fermilab", issuer)
        
        #if ctx.experiment not in ["samdev","accel","accelai", "icarus","admx", "annie","argoneut","cdms","chips","cms","coupp","darksectorldrd","darkside","ebd","egp","emph","emphatic","fermilab","genie","lariat","larp","magis100","mars","minerva","miniboone","minos","next","noble","nova","numix","patriot","pip2","seaquest","spinquest","test","theory","uboone"]:
        #    oidcpath = oidcpath.replace("fermilab", ctx.experiment)
        #    issuer = ctx.experiment
        if ctx.role != "analysis":
            role = "%spro" % ctx.experiment
        
        path = '/v1/' + oidcpath + '/auth_url'
        url = vaultserver + path
        nonce = secrets.token_urlsafe()
        authdata = {
            'role': role,
            'client_nonce': nonce,
            'redirect_uri': ctx.web_config.get("tokens", "redirect_uri").replace("PLACEHOLDER1", oidcpath)
        }
        data = json.dumps(authdata)
        if debug:
            logit.log("Authenticating to", url)
            logit.log("##### Begin authentication data")
            logit.log(data)
        body = None
        try:
            logit.log("##### Trying to get Response")
            resp = requests.post(url, data = data)
            body = resp.content.decode(encoding='utf-8', errors='strict') 
        except Exception as e:
            logit.log("Initiating authentication to %s failed: %s" % (vaultserver, repr(e)))
        
        if debug:
            logit.log("##### Begin vault initiate auth response")
            logit.log(body)
            logit.log("##### End vault initiate auth response")
        try:
            response = json.loads(body)
        except Exception as e:
            logit.log("decoding response from %s failed" % vaultserver, e)

        data = {}
        if 'data' not in response:
            logit.log("no 'data' in response from %s" % vaultserver)
        else:
            data = response['data']
        if 'auth_url' not in data:
            logit.log("no 'auth_url' in data from %s" % vaultserver)
            
        if debug:
            auth_url = data.get("auth_url", referer)
            if auth_url == "":
                logit.log("'auth_url' is empty in data from %s" % vaultserver)

            logit.log("Complete the authentication at:")
            logit.log("    " + auth_url)
        
        data['oidcpath'] = oidcpath  
        data['role'] = role
        data['client_nonce'] = nonce
        data['vaultserver'] = vaultserver
        data['debug'] = str(debug)
        data['issuer'] = issuer
        data['env'] = ctx.role.lower()
        if referer:
            data['referer'] = referer
        return data
         

    def poll_oidc_url(self, ctx, **kwargs):
        strdata = kwargs.get("data", None)
        strdata = strdata.replace("'", "\"")
        data = json.loads(strdata)
        pollinterval = 0
        logit.log("Data = " + repr(data))
        oidcpath = data['oidcpath']
        role = data['role']
        role = ctx.role
        vaultserver = data['vaultserver']
        debug = data['debug'] == "True"
        redir = kwargs.get("redir", None)
        
        if debug:
            logit.log("redir: %s" % redir)

        datastr = ''
        if 'state' in data:
            path = '/v1/' + oidcpath + '/poll'
            if 'poll_interval' in data:
                pollinterval = int(data['poll_interval'])
                del data['poll_interval']
        else:
            # backward compatibility for old device flow implementation
            path = '/v1/' + oidcpath + '/device_wait'
            data['role'] = role
        url = vaultserver + path
        datastr = json.dumps(data)
        if debug:
            logit.log("Continuing authentication at", url)
            if datastr != '':
                logit.log("##### Begin continuation data")
                logit.log(datastr)
                logit.log("##### End continuation data")

        response = None
        secswaited = 0
        body = None
        while True:
            try:
                if secswaited > 120:
                    logit.log("Polling for response took longer than 2 minutes")
                if debug:
                    logit.log("waiting for " + str(pollinterval) + " seconds")
                time.sleep(pollinterval)
                secswaited += pollinterval
                if debug:
                    logit.log("Polling Body: %s" % data)
                # The normal "authorized_pending" response comes in
                #  the body of a 400 Bad Request.  If we let the
                #  exception go as normal, the resp is not set and we
                #  can't read the body, so temporarily block 400 from
                #  throwing an exception.
                
                resp = requests.post(url, data = data)
                
                body = resp.content.decode(encoding='utf-8', errors='strict') 
                
                #resp = requests.post(path, data = datastr.encode()).response() #self.vault.request(path, data=datastr.encode(), ignore_400=True)
            except Exception as e:
                logit.log("Authentication to %s failed: %s" % (vaultserver, repr(e)))
            if debug:
                logit.log("##### Begin vault auth response")
                logit.log(body)
                logit.log("##### End vault auth response")
            try:
                response = json.loads(body)
            except Exception as e:
                logit.log("decoding response from %s failed" % vaultserver, e)
            if 'errors' in response:
                errors = response['errors']
                if errors[0] == "slow_down":
                    pollinterval = pollinterval * 2
                elif errors[0] != "authorization_pending":
                    logit.log("error in response from %s: %s" % (vaultserver, errors[0]))
                if debug:
                    logit.log("authorization pending, trying again")
            else:
                # good reply
                data['auth_url'] = ''
                break

        logit.log("Response: %s" % response)
        
        path = f"/run/user/{os.geteuid()}"
        vaultpath = "/home/poms/uploads/%s/%s" % (ctx.experiment, ctx.username)
        if role == "analysis":
            tokenfile = f"{path}/bt_{ctx.experiment}_analysis_{ctx.username}"
            vaultfile = f"vt_{ctx.experiment}_Analysis_{ctx.username}"
        else:
            tokenfile = f"{path}/bt_{ctx.experiment}_production_{ctx.username}"
            vaultfile = f"vt_{ctx.experiment}_production_{ctx.username}"
        
        data["tokenfile"] = tokenfile
        data["vaultfile"] = vaultfile
        vaultpath = "%s/%s" % (vaultpath, vaultfile)

        secretpath = "secret/oauth/creds/issuer/credkey:role"
        secretpath = secretpath.replace("issuer", data['issuer'])
        secretpath = secretpath.replace("role", data['role'])
        fullsecretpath = secretpath.replace("credkey", ctx.username)
        
        data["fullsecretpath"] = fullsecretpath
        
        #if self.try_kerb_auth(ctx, data, vaultserver, data['issuer']):
        #    logit.log("successfully did kerb auth")
        #else:
        #    logit.log("failed kerb auth")

        vaulttokensecs = self.ttl2secs("28d", "--vaulttokenttl")
        vaulttoken = self.getVaultToken(vaulttokensecs, response, data)
        
        logit.log("Saving vault token")
        
        self.writeTokenSafely("vault", vaulttoken, vaultpath)
        self.writeTokenSafely("vault", vaulttoken, "/tmp/vt_u%s" % os.geteuid())
        self.writeTokenSafely("vault", vaulttoken, "/tmp/vt_u%s-%s" % (os.geteuid(), ctx.experiment))
        try:
            logit.log("Saved and attempting to modify vault token permissions")
            os.chmod(vaultpath, stat.S_IROTH |stat.S_IRGRP| stat.S_IRUSR | stat.S_IWUSR)
            os.system("touch %s" % (vaultpath))
            logit.log("Saved and modified vault token permissions %s" % datetime.now().strftime("%H:%M:%S.%f") )
        except Exception as e:
            logit.log("failure updating permissions: %s" % str(e))   
                
                
        if debug:      
            try:
                logit.log("View tmp permissions")
                pr = subprocess.Popen(["ls -l /tmp"], stdout=subprocess.PIPE, shell=True)
                logit.log("TMP Permissions: %s" % pr.stdout.read().decode('utf8'))
            except Exception as e:
                logit.log("failure checking tmp permissions: %s" % str(e))   
        

        auth = response['auth']

        if 'metadata' not in auth:
            logit.log("no 'metadata' in response from %s" % vaultserver)
        metadata = auth['metadata']
        if 'credkey' not in metadata:
            logit.log("no 'metadata' in response from %s" % vaultserver)

        credkey = metadata['credkey']
        if debug:
            logit.log("error cra: %s" % metadata)

        try:
            os.makedirs(os.path.expanduser("/home/poms/.config/htgettoken"), exist_ok=True)
        except Exception as e:
            logit.log("error creating /home/poms/.config/htgettoken/credkey-%s-%s: %s" % (data['issuer'], ctx.username, repr(e)))
        try:
            with open("/home/poms/.config/htgettoken/credkey-%s-%s" % (data['issuer'], ctx.username), 'w') as file:
                file.write(credkey + '\n')
        except Exception as e:
            logit.log('error writing %s: %s' % ("/home/poms/.config/htgettoken-%s-%s" % (data['issuer'], ctx.username), repr(e)))
        
        if 'oauth2_refresh_token' not in metadata:
            logit.log("no 'oauth2_refresh_token' in response from %s" % vaultserver)

        refresh_token = metadata['oauth2_refresh_token']

        
        
        logit.log("Saving refresh token to " + vaultserver)
        logit.log("  at path " + fullsecretpath)

        path = '/v1/' + fullsecretpath
        url = vaultserver + path
        headers = {'X-Vault-Token': vaulttoken}
        storedata = {
            'server': data['issuer'],
            'refresh_token': refresh_token
        }
        if debug:
            logit.log("Refresh token url is", url)
            logit.log("##### Begin refresh token storage data")
            logit.log(storedata)
            logit.log("##### End refresh token storage data")
        try:
            resp = requests.get(url, headers=headers, data=storedata)
            body = resp.content.decode(encoding='utf-8', errors='strict')
            if debug:
                logit.log("Refresh token stored body = %s" % body)
        except Exception as e:
            logit.log("Refresh token storage to %s failed: %s" % (vaultserver, repr(e)))

        bearertoken = self.getBearerToken(vaulttoken, fullsecretpath, data)

        if bearertoken is None:
            logit.log("Failure getting token from " + vaultserver)

        # Write bearer token to outfile
        self.writeTokenSafely("bearer", bearertoken, tokenfile)

        return data
        


    def ttl2secs(self, ttl, msg):
        # calculate ttl in seconds
        lastchr = ttl[-1:]
        numpart = ttl[0:-1]
        failmsg = msg + " is not a number followed by s, m, h, or d"
        if not numpart.isnumeric():
            logit.log(failmsg)
        secs = int(numpart)
        if lastchr == 'd':
            secs *= 24
            lastchr = 'h'
        if lastchr == 'h':
            secs *= 60*60
        elif lastchr == 'm':
            secs *= 60
        elif lastchr != 's':
            logit.log(failmsg)
        return secs

    def isDevFile(self, file):
        return file.startswith("/dev/std") or file.startswith("/dev/fd")

    # safely write out a token to where it might be a world-writable
    #  directory, unless the output is a device file
    def writeTokenSafely(self, tokentype, token, outfile):
        dorename = False
        handle = None
        path = ""
        if self.isDevFile(outfile):
            logit.log("Witing %s token to %s" % (tokentype, outfile))
            try:
                handle = open(outfile, 'w')
            except Exception as e:
                logit.log("failure opening for write: %s" % repr(e))
        else:
            logit.log("Storing %s token in %s" % (tokentype, outfile))
            # Attempt to remove the file first in case it exists, because os.O_EXCL
            #  requires it to be gone.  Need to use os.O_EXCL to prevent somebody
            #  else from pre-creating the file in order to steal credentials.
            try:
                os.remove(outfile)
            except:
                pass
            try:
                fd, path = tempfile.mkstemp(
                    prefix=os.path.dirname(outfile) + '/.' + "htgettoken")
                handle = os.fdopen(fd, 'w')
            except Exception as e:
                logit.log("failure creating file: %s" % repr(e))
            dorename = True

        try:
            handle.write(token + '\n')
        except Exception as e:
            logit.log("failure writing file: %s" % repr(e))
        if handle:
            handle.close()
        if dorename:
            try:
                os.rename(path, outfile)
            except Exception as e:
                try:
                    os.remove(outfile)
                except:
                    pass
                logit.log("failure renaming " + path + " to " + outfile, e)
        
            
        
    

    def getVaultToken(self, vaulttokensecs, response, data):
        vaultserver = data['vaultserver']
        if 'auth' not in response:
            logit.log("no 'auth' in response from %s" % vaultserver)
        auth = response['auth']

        if 'client_token' not in auth:
            logit.log("no 'client_token' in response from %s" % vaultserver)
        vaulttoken = auth['client_token']

        policies = None
        if 'policies' in auth:
            for policy in auth['policies']:
                if policy.startswith('sshregister'):
                    policies = auth['policies']
                    policies.remove(policy)
                    break

        if 'lease_duration' in auth and int(auth['lease_duration']) <= vaulttokensecs \
                and policies is None:
            # don't need to exchange, already the correct duration or shorter
            return vaulttoken

        # do a vault token exchange
        path = '/v1/' + 'auth/token/create'
        url = vaultserver + path
        if data['debug'] == "True":
            # normally do this quietly; don't want to advertise the exchange
            logit.log("Reading from", url)
        headers = {'X-Vault-Token': vaulttoken}
        formdata = {
            'ttl': "7d",
            'renewable': 'false',
        }
        if policies is not None:
            data['policies'] = policies

        body = None
        try:
            resp = requests.post(url, headers=headers, data=formdata)
            body = resp.content.decode(encoding='utf-8', errors='strict')
            #logit.log("Body = %s" % body)
        except Exception as e:
            logit.log("getting vault token from %s failed: %s" % (url, e))
        
        if data['debug'] == "True":
            logit.log("##### Begin vault token response")
            logit.log("Body = %s" % body)
            logit.log("##### End vault token response")
        try:
            response = json.loads(body)
        except Exception as e:
            logit.log("decoding response from %s failed" % url, e)
        if 'auth' in response and 'client_token' in response['auth']:
            return response['auth']['client_token']
        logit.log("no vault token in response from %s" % url)


    def getBearerToken(self, vaulttoken, vaultpath, data):
        
        vaultserver = data['vaultserver']
        logit.log("  at path " + vaultpath)

        path = '/v1/' + vaultpath
        url = vaultserver + path
        params = {'minimum_seconds': 60}
        #if scopes is not None:
        #    params['scopes'] = options.scopes
        #if audience is not None:
        #    params['audience'] = options.audience
        if data['debug'] == "True":
            logit.log("Reading from", url)
        headers = {'X-Vault-Token': vaulttoken}
        body = None
        try:
            resp = requests.get(url, headers=headers, params=params)
            body = resp.content.decode(encoding='utf-8', errors='strict')
        except Exception as e:
            logit.log("Read token from %s failed" % url, e)
            return None
        if data['debug'] == "True":
            logit.log("##### Begin vault get bearer token response")
            logit.log(body)
            logit.log("##### End vault get bearer token response")
        try:
            response = json.loads(body)
        except Exception as e:
            logit.log("decoding response from %s failed" % url, e)
        if 'data' in response and 'access_token' in response['data']:
            return response['data']['access_token']
        
        return None
    
    # TODO: If we ever decide to use this, this function will need to be fixed to work.
    # This functionwas in development before we decided to exclusively use poms_client for analysis tokens.
    def try_kerb_auth(self, ctx, data, vaulthostname, issuer):
         # Try kerberos authentication with vault
        logit.log("try_kerb_auth")
        vaultserver = data['vaultserver']
        logit.log(vaultserver)
        debug = data["debug"] == "True"
        service = "host@htvaultprod"
        
        if debug:
            logit.log("Initializing kerberos client for: " + service)

        # Need to disable kerberos reverse DNS lookup in order to
        #  work properly with server aliases
        cfgfile = tempfile.NamedTemporaryFile(mode='w')
        if debug:
            logit.log("Disabling kerberos reverse DNS lookup in " + cfgfile.name) 
        cfgfile.write("[libdefaults]\n    rdns = false\n")
        cfgfile.flush()
        
        s = subprocess.call("klist -A", shell=True)
        logit.log("klist: %d %s" % (s, ctx.experiment))
        os.environ["$KRB5CCNAME"] = "/tmp/krb5cc_poms_auth_%s" % ctx.experiment
        os.environ["$X509_USER_PROXY"] = "/home/poms/uploads/%s/%s/x509up_voms_%s_Analysis_%s" % (ctx.experiment, ctx.username, ctx.experiment, ctx.username)
        os.environ["$REQUESTS_CA_BUNDLE"] = os.environ["$X509_USER_PROXY"]
        os.system("kinit -X X509_user_identity=$X509_USER_PROXY -kt $HOME/private/keytabs/poms.keytab `klist -kt $HOME/private/keytabs/poms.keytab | tail -1 | sed -e 's/.* //'`|| true;")
        
        krb5_config = None #"/etc/krb5.conf"
        
        if krb5_config is None:
            # Try not reading from /etc/krb5.conf because it can
            # interfere if the kerberos domain is missing
            os.environ["KRB5_CONFIG"] = cfgfile.name
            #os.environ["KRB5_CONFIG"] = "/etc/krb5.conf"
        else:
            #os.environ["KRB5_CONFIG"] = "/etc/krb5.conf"
            os.environ["KRB5_CONFIG"] = cfgfile.name + ':' + krb5_config
            
        if debug:
            logit.log("Setting KRB5_CONFIG=" + os.getenv("KRB5_CONFIG"))
        kname = None
    
        try:
            kname = gssapi.Name(base=service, name_type=gssapi.NameType.hostbased_service)
        except Exception as e:
            logit.log("Error: %s" % repr(e))
        
        os.environ["$X509_USER_PROXY"] = "%s/x509up_voms_%s_Analysis_%s" % ("/home/poms/uploads/%s/%s" % (ctx.experiment, ctx.username), ctx.experiment, ctx.username)
        kcontext = gssapi.SecurityContext(usage="initiate", name=kname)
        kresponse = None
        logit.log("Proxy = " + os.environ["$X509_USER_PROXY"])
        try:
            kresponse = kcontext.step()
        except Exception as e:
            if krb5_config is None and (len(e.args) != 2 or \
                    len(e.args[1]) != 2 or 'expired' not in e.args[1][0]):
                # Try again with the default KRB5_CONFIG because 
                # krb5.conf might be there and might work better.
                # Don't do it for expired tickets because those have
                # been observed to not always get caught with 2nd try.
                if debug:
                    logit.log("Kerberos init without /etc/krb5.conf failed", e)
                    logit.log("Trying again with /etc/krb5.conf")
                os.environ["KRB5_CONFIG"] = cfgfile.name + ":/etc/krb5.conf"
                if debug:
                    logit.log("Setting KRB5_CONFIG=" + os.getenv("KRB5_CONFIG"))
                kcontext = gssapi.SecurityContext(usage="initiate", name=kname)
                try:
                    kresponse = kcontext.step()
                except Exception as e2:
                    logit.log("Kerberos failed: %s" % repr(e2))
                    kresponse = None
                    e = e2
            if kresponse is None:
                logit.log("Kerberos init failed: %s" % traceback.format_exc() )
            else:
                logit.log("Kerberos init failed")

        cfgfile.close()

        if kresponse != None:
            kerberostoken = base64.b64encode(kresponse).decode()
            if debug:
                logit.log("Kerberos token: %s" % kerberostoken)
                
            kerbpath = "auth/kerberos-%issuer_%role"
            kerbpath = kerbpath.replace("%issuer", issuer)
            #kerbpath = kerbpath.replace("%issuer", "fermilab")
            kerbpath = kerbpath.replace("%role", ctx.role if ctx.role == "production" else "default")
            path = "/v1/" + kerbpath + '/login'
            url = vaultserver + path
            logit.log("Negotiating kerberos with", vaultserver)
            logit.log("  at path " + kerbpath)
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Negotiate ' + kerberostoken
            }
            formdata = ''.encode('ascii')  # empty data is to force a POST
            try:
                os.system("export USER=ltrestka")
                resp = requests.post(url, headers=headers, data=formdata)
                body = resp.content.decode(encoding='utf-8', errors='strict') 
            except Exception as e:
                logit.log("Kerberos negotiate with %s failed: %s" % (url, repr(e)))

            body = resp.content.decode(encoding='utf-8', errors='strict') 
            if debug:
                logit.log("##### Begin vault kerberos response")
                logit.log(body)
                logit.log("##### End vault kerberos response")
            response = json.loads(body)
            if 'auth' in response and response['auth'] is not None:
                if debug:
                    logit.log(" succeeded")
                response["auth"]["metadata"]["user"] = "ltrestka"
                vaulttokensecs = self.ttl2secs("28d", "--vaulttokenttl")
                logit.log("getting vault token with kresponse: %s" % repr(response))
                vaulttoken = self.getVaultToken(vaulttokensecs, response, data)
                logit.log("got vault token: %s" %repr(vaulttoken))
                logit.log("Attempting to get bearer token from " + vaultserver)

                #bearertoken = self.getBearerToken(vaulttoken, data["fullsecretpath"], data)

                #if bearertoken is not None:
                    # getting bearer token worked, write out vault token
                self.writeTokenSafely("vault", vaulttoken, data["vaultfile"])
                self.writeTokenSafely("vault", vaulttoken, "/tmp/vt_u%s" % os.geteuid())
                self.writeTokenSafely("vault", vaulttoken, "/tmp/vt_u%s-%s" % (os.geteuid(), ctx.experiment))
                    #self.writeTokenSafely("bearer", vaulttoken, data["tokenfile"])
                s = subprocess.call("condor_vault_storer -v -d %s" % ctx.experiment, shell=True)
                logit.log("condor_vault_storer: %d %s" % (s, ctx.experiment))
                return True
                
                return False

            else:
                logit.log("Kerberos authentication failed")
                if debug:
                    if 'warnings' in response:
                        for warning in response['warnings']:
                            logit.log("  " + warning)
                    if 'errors' in response:
                        for error in response['errors']:
                            logit.log("  " + error)
                return False
        return False
