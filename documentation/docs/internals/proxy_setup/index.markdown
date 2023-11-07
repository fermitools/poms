---
layout: page
title: Proxy Setup
---
* TOC
{:toc}
We have certificate authentication up and running on: 
* [https://pomsgpvm01.fnal.gov:8443/poms/](https://pomsgpvm01.fnal.gov:8443/poms/)
* [https://fermicloud210.fnal.gov:8443/poms/](https://fermicloud210.fnal.gov:8443/poms/)  

Thanks to the Magic of Apache's RewriteCond/RewriteRule and regular expressions*,  
we have the same headers (approximately) that we have for Shibboleth authentication, so no (?) code changes are required to the poms code.

Here's the magic bits, for those who care:

    RewriteCond %{SSL:SSL_CLIENT_S_DN} "^/.*/CN=(\S*)\s([^/]*)/CN=UID:(\w*)"
    RewriteRule .* - [E=HTTP_X_SHIB_NAME_FIRST:%1,E=HTTP_X_SHIB_NAME_LAST:%2,E=HTTP_
    X_SHIB_USERID:%3,E=HTTP_X_SHIB_EMAIL:%3@fnal.gov]
    RequestHeader set X_SHIB_USERID %{HTTP_X_SHIB_USERID}e
    RequestHeader set X_SHIB_EMAIL %{HTTP_X_SHIB_EMAIL}e
    RequestHeader set X_SHIB_NAME_LAST %{HTTP_X_SHIB_NAME_LAST}e