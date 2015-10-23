#!/usr/bin/env python

import os

class jobsub_fetcher():
    def __init__(self):
         self.workdir = "%s/jf%d" % (
                          os.environ.get("TMPDIR","/var/tmp"),
                          os.getpid())
         os.mkdir(self.workdir)
         self.fetchmax = 10
         self.fetchcount = 0
         self.tarfiles = []

    def __del__(self):
         if self.workdir:
              os.system("rm -rf %s & " % self.workdir)

    def fetch(self, jobsubjobid, group, role):
         thistar = "%s/%s.tgz" % (self.workdir, jobsubjobid)
         if os.path.exists(thistar):
             return

         self.fetchcount = self.fetchcount + 1
         self.tarfiles.append(thistar)

         if self.fetchcount > self.fetchmax:
             os.unlink(self.tarfiles[0])
             self.tarfiles = self.tarfiles[1:]
             self.fetchcount = self.fetchcount - 1   
                 
         os.system("cd %s && /bin/pwd && jobsub_fetchlog --group=%s --role=%s --jobid=%s" %(
                     self.workdir,
                     group,
                     role,
                     jobsubjobid))
         os.system("ls -l %s " % self.workdir)

    def index(self, jobsubjobid, group, role = "Production"):
        self.fetch(jobsubjobid, group, role)
        f = os.popen( "tar tzf %s/%s.tgz" % (self.workdir, jobsubjobid), "r")
        res = []
        for line in f:
             res.append(line.rstrip('\n'))
        f.close()
        return res

    def contents(self, filename, jobsubjobid, group, role = "Production"):
        self.fetch(jobsubjobid, group, role)
        f = os.popen( "tar --to-stdout -xzf %s/%s.tgz %s" % (self.workdir, jobsubjobid, filename), "r")
        res = []
        for line in f:
            res.append(line.rstrip('\n'))
        f.close()
        return res

if __name__ == "__main__":
     
    jf = jobsub_fetcher()
    jobid="4977156.0@fifebatch2.fnal.gov"
    flist = jf.index(jobid, "nova", "Analysis") 
    print "------------------"
    print flist
    print "------------------"
    print jf.contents(flist[2], jobid, "nova", "Analysis")
