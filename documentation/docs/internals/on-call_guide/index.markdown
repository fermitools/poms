---
layout: page
title: On-call Guide
---
* TOC
{:toc}
This is a draft of an on-call guide for POMS; please feel free to ask questions, add updates, etc.

## Overall

* POMS Processes / Services currently runs
  * On **pomsgpvm01.fnal.gov**
  * Under the **poms** account
  * Managed by [supervisor](http://supervisord.org/)
* Most web usage is fronted by the system Apache, of which we have limited config ability, by editing files in /etc/httpd/conf.d and using sudo to restart apache (see below).
* Supervisor config is in **$HOME/supervisord.conf**
* Poms software is currently installed via Spack areas in /home/poms/packages, as is supervisor itself. There is a shortcut symlink to the poms code in the home area.
* Logs are under **$HOME/logs/poms**
* Config files are under **$HOME/config/poms**


## Checklist

* Check main page [https://pomsgpvm01.fnal.gov/poms/](https://pomsgpvm01.fnal.gov/poms/)
* Check: "ssh poms@pomsgpvm01 bin/health_check" should have
  * 4-digits free memory
  * 2-digits idle cpu
  * Ok: recent job updates
  * Backtraces hopefully zero, but in 3 digits probably ok.

Backtrace reason:

    sqlalchemy.exc.UnboundExecutionError: Could not locate a bind configured on SQL expression or this Session


Usually needs an application restart to clear (db handle leak?)


## If things aren't running, or database session errors

1. Make a Servicedesk ticket, or mark existing one "Work in Progress"
2. Log into the server **ssh -l poms pomsgpvm01.fnal.gov**; if you can't get in; try and **ping pomsgpvm01.fnal.gov**, and in any case make a ticket to Scientific Server Support tog get it restarted.
3. Check memory usage **top**: we ought to have about 1/6 of our memory free, and have double digit idle cpu.
4. Check if services are running **cd $HOME; supervisorctl status**, if not **supervisorctl start service**
5. Check for exceptions, etc. **cd $HOME/logs/poms ; grep ' line ' error.log**
6. Try to restart webservice **cd $HOME; supervisorctl restart poms_webservice**
    * if it doesn't start, try **killall uwsgi and supervisorctl start poms_webservice**
7. Try to restart apache **sudo /etc/init.d/httpd restart**
8. If all else fails, cut a ticket to Scientific Server Support and ask them to reboot the VM.


## Possible fixes

#### If a user is getting an authorization error for analysis submissions, something that looks like:

    ERROR:
    User authorization has failed: Error authenticating DN='/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=John Doe/CN=UID:jdoe' for AcctGroup='someexp'


This could mean that the user doesn't have a valid proxy in MyProxy.

Have them run a

    jobsub_q -G mu2e --user boyd

on a command line. This will make jobsub upload a proxy to MyProxy if one isn't there and then POMS submissions may work.