#!/usr/bin/env python


# This module contain the methods that handle the Calendar. (5 methods)
# List of methods: handle_dates, quick_search, jump_to_job,
# Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
# written by Marc Mengel, Stephen White and Michael Gueith.
### October, 2016.
from datetime import datetime, timedelta
from .utc import utc
from .poms_model import Experimenter, ExperimentsExperimenters
import tempfile
import requests
import secrets
import json
import time
import os
from . import logit


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

    def get_oidc_url(self, ctx, vaultserver = "https://htvaultprod.fnal.gov:8200", oidcpath = "auth/oidc-fermilab/oidc", debug = True):
        logit.log("Attempting OIDC authentication")
        role = "default"
        if ctx.experiment != "samdev":
            oidcpath = oidcpath.replace("fermilab", ctx.experiment)
            if ctx.role != "analysis":
                role = "%spro" % ctx.experiment
        path = '/v1/' + oidcpath + '/auth_url'
        url = vaultserver + path
        nonce = secrets.token_urlsafe()
        authdata = {
            'role': role,
            'client_nonce': nonce,
            'redirect_uri': vaultserver + '/v1/https:/cilogon.org/fermilab/callback'
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

        if 'data' not in response:
            logit.log("no 'data' in response from %s" % vaultserver)
        data = response['data']
        if 'auth_url' not in data:
            logit.log("no 'auth_url' in data from %s" % vaultserver)
        auth_url = data['auth_url']
        #del data['auth_url'] 
        if auth_url == "":
            logit.log("'auth_url' is empty in data from %s" % vaultserver)

        logit.log("Complete the authentication at:")
        logit.log("    " + auth_url)
        data['oidcpath'] = oidcpath
        data['role'] = role
        data['client_nonce'] = nonce
        data['vaultserver'] = vaultserver
        data['debug'] = str(debug)
        data['issuer'] = "fermilab" if ctx.experiment == "samdev" else ctx.experiment
        data['env'] = ctx.role.lower()
        return data
         

    def poll_oidc_url(self, ctx, **kwargs):
        strdata = kwargs.get("data", None)
        strdata = strdata.replace("'", "\"")
        data = json.loads(strdata)
        pollinterval = 0
        logit.log("Data = " + repr(data))
        oidcpath = data['oidcpath']
        role = data['role']
        vaultserver = data['vaultserver']
        debug = data['debug'] == "True"

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
                    logit.log("polling")
                # The normal "authorized_pending" response comes in
                #  the body of a 400 Bad Request.  If we let the
                #  exception go as normal, the resp is not set and we
                #  can't read the body, so temporarily block 400 from
                #  throwing an exception.
                logit.log("Polling Body: %s" % data)
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

        vaulttokensecs = self.ttl2secs("7d", "--vaulttokenttl")
        vaulttoken = self.getVaultToken(vaulttokensecs, response, data)


        self.writeTokenSafely("vault", vaulttoken, "/tmp/%s_%s_vt_u%d" % (data['issuer'], data['env'], os.geteuid()))
        auth = response['auth']

        if 'metadata' not in auth:
            logit.log("no 'metadata' in response from %s" % vaultserver)
        metadata = auth['metadata']
        if 'credkey' not in metadata:
            logit.log("no 'metadata' in response from %s" % vaultserver)

        credkey = metadata['credkey']
        logit.log("error cra: %s" % metadata)

        try:
            os.makedirs(os.path.expanduser("~/.config/htgettoken"), exist_ok=True)
        except Exception as e:
            logit.log('error creating %s' % "~/.config/htgettoken", e)
        
        if 'oauth2_refresh_token' not in metadata:
            logit.log("no 'oauth2_refresh_token' in response from %s" % vaultserver)

        refresh_token = metadata['oauth2_refresh_token']

        secretpath = "secret/oauth/creds/%issuer/%credkey:%role"
        secretpath = secretpath.replace("%issuer", data['issuer'])
        secretpath = secretpath.replace("%role", data['role'])
        fullsecretpath = secretpath.replace("%credkey", credkey)

        logit.log("Saving refresh token to " + vaultserver)
        logit.log("  at path " + fullsecretpath)

        path = '/v1/' + fullsecretpath
        url = vaultserver + path
        headers = {'X-Vault-Token': vaulttoken}
        storedata = {
            'server': data['issuer'],
            'refresh_token': refresh_token
        }
        if data['debug'] == "True":
            logit.log("Refresh token url is", url)
            logit.log("##### Begin refresh token storage data")
            logit.log(storedata)
            logit.log("##### End refresh token storage data")
        try:
            resp = requests.post(url, headers=headers, data=storedata)
        except Exception as e:
            logit.log("Refresh token storage to %s failed" % vaultserver, e)

        bearertoken = self.getBearerToken(vaulttoken, fullsecretpath, data)

        if bearertoken is None:
            logit.log("Failure getting token from " + vaultserver)

        # Write bearer token to outfile
        self.writeTokenSafely("bearer", bearertoken, "/run/user/%d/%s_%s_bt_u%d" % (os.geteuid(),data['issuer'], data['env'], os.geteuid()))

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
        if self.isDevFile(outfile):
            logit.log("Writing", tokentype, "token to", outfile)
            try:
                handle = open(outfile, 'w')
            except Exception as e:
                logit.log("failure opening for write", e)
        else:
            logit.log("Storing", tokentype, "token in", outfile)
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
                logit.log("failure creating file", e)
            dorename = True

        try:
            handle.write(token + '\n')
        except Exception as e:
            logit.log("failure writing file", e)
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
            logit.log("Body = %s" % body)
        except Exception as e:
            logit.log("getting vault token from %s failed" % url, e)
        
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
