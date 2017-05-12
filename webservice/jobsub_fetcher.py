#!/usr/bin/env python

import os
from os import system as os_system
import _thread
from .logit import log

class jobsub_fetcher():
    def __init__(self):
         self.workdir = "%s/jf%d%s" % (
                          os.environ.get("TMPDIR","/var/tmp"),
                          os.getpid(),
                          _thread.get_ident())
         try:
             os.mkdir(self.workdir)
         except:
             pass
         self.fetchmax = 10
         self.fetchcount = 0
         self.tarfiles = []

    def flush(self):
         if self.workdir:
              os_system("rm -rf %s" % self.workdir)

    def fetch(self, jobsubjobid, group, role, force_reload = False, user = None):
         if group == "samdev": group = "fermilab"
         thistar = "%s/%s.tgz" % (self.workdir, jobsubjobid.rstrip("\n"))
         if os.path.exists(thistar):
             if force_reload:
                 os.unlink(thistar)
             else:
                 return
         else:
             self.fetchcount = self.fetchcount + 1
             self.tarfiles.append(thistar)

         if self.fetchcount > self.fetchmax:
             try:
                 os.unlink(self.tarfiles[0])
             except:
                 pass
             self.tarfiles = self.tarfiles[1:]
             self.fetchcount = self.fetchcount - 1   

             if user == None:
                user = "%spro" % group

         cmd = "cd %s && /usr/bin/python $JOBSUB_CLIENT_DIR/jobsub_fetchlog --group=%s --role=%s --user=%s --jobid=%s > /dev/null" %(
                     self.workdir,
                     group,
                     role,
                     user,
                     jobsubjobid)
         
         log("jobsub_fetcher.fetch(): running: %s" % cmd)
         os.system(cmd)
         # os.system("ls -l %s " % self.workdir)

    def index(self, jobsubjobid, group, role = "Production", force_reload = False):

        if group == "samdev": group = "fermilab"
        self.fetch(jobsubjobid, group, role, force_reload)
        f = os.popen( "tar tvzf %s/%s.tgz" % (self.workdir, jobsubjobid.rstrip("\n")), "r")
        res = []
        for line in f:
             res.append(line.rstrip('\n').split())
        f.close()
        return res

    def contents(self, filename, jobsubjobid, group, role = "Production"):
        self.fetch(jobsubjobid, group, role)
        f = os.popen( "tar --to-stdout -xzf %s/%s.tgz %s" % (self.workdir, jobsubjobid.rstrip("\n"), filename), "r")
        res = []
        for line in f:
            res.append(line.rstrip('\n'))
        f.close()
        return res

if __name__ == "__main__":
     
    jf = jobsub_fetcher()
    jobid="15200109.0@fifebatch2.fnal.gov"
    flist = jf.index(jobid, "samdev", "Analysis") 
    print("------------------")
    print(flist)
    print("------------------")
    print(jf.contents(flist[2][-1], jobid, "samdev", "Analysis"))
