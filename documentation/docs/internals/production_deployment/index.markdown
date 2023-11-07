---
layout: page
title: Production Deployment
---
* TOC
{:toc}

* POMS Processes / Services currently run on on host **pomsgpvm01.fnal.gov**, under the **poms** account, and are managed by [supervisord](http://supervisord.org/)
* Most web usage is fronted by the system Apache, of which we have limited config ability, by editing files in /etc/httpd/conf.d and using sudo to restart apache (see below).
* Supervisord config is in $HOME/supervisord.conf
* Poms software (including supervisord) is currently installed via ups/upd $HOME/products.
* Logs are under **$HOME/private/logs/poms**
* Config files are under **$HOME/private/config/poms**

Currently we are running under uwsgi, with the following important bits in **$HOME/poms/private/config/poms/uwsg-service.ini**:

* Min, initial, and Max worker processes to start:
  * cheaper = 2
  * cheaper-initial = 4
  * workers = 6
* Memory limits -- we've been leaking memory, so restart nicely at 1.4G, kill it if it goes over 1.8
  * reload-on-as = 1400
  * **evil-reload-on-as = 1800
* 10 threads per process, so we can have up to 10 threads * 6 processes = 60 requests being handled in parallel and possibly waiting on the database, or on a job launch, etc.
  * threads = 10

The uwsgi instances are fronted by the system Apache, and there are several apache configs involved.  
Here I'm documenting what's on fermicloud210, since we'll be setting up pomsgpvm01 that way soon.

In **/etc/httpd/conf.d**, we have some config in **ssl.conf** require shibboleth authentication /poms/* on port 443:

    <Location /poms>
        AuthType shibboleth
        ShibCompatWith24 On
        ShibRequestSetting requireSession 1
        require shib-session
        ShibBasicHijack On
        require valid-user
        ShibUseEnvironment On
    </Location>

This needs to be in the virtual host block, 'cause we also want a port 8443 vhost that uses kx509 certificates
to authenticate.  
That is in **cert_ssl.conf**, which needs to be largely identical to the regular ssl.conf, excepting:

    Listen 8443
    ...
    SSLVerifyClient require
    SSLVerifyDepth  10
    SSLCACertificatePath /etc/httpd/TrustedCAs
    ...
    RewriteEngine on

    RewriteCond %{SSL:SSL_CLIENT_S_DN} "^/.*/CN=(\S*)\s([^/]*)/CN=UID:(\w*)"RewriteRule .* - [=HTTP_X_SHIB_NAME_FIRST:%1,E=HTTP_X_SHIB_NAME_LAST:%2,E=HTTP_X_SHIB_USERID:%3,E=HTTP_X_SHIB_EMAIL:%3@fnal.gov]

    RequestHeader set X_SHIB_USERID %{HTTP_X_SHIB_USERID}e
    RequestHeader set X_SHIB_EMAIL %{HTTP_X_SHIB_EMAIL}e
    RequestHeader set X_SHIB_NAME_LAST %{HTTP_X_SHIB_NAME_LAST}e
    RequestHeader set X_SHIB_NAME_FIRST %{HTTP_X_SHIB_NAME_FIRST}e

The Rewrite and Request rules basically fake the headers we would pass in if we were using shibboleth, authentication; instead based on the users certificate data. NOTE: this may require an extra rule or two for production certificates that don't have a CN=UID:username part in them later...