import os
import sys

class Mock_jobsub_rm:
     def __init__(self):
         ##Moving
         os.environ['PATH'] = "%s/test/bin_mock_jobsub_rm:%s" % (
                os.environ['POMS_DIR'],os.environ['PATH']
             )

     def close(self):
         os.environ['PATH'] = os.environ['PATH'].replace(
                 "%s/test/bin_mock_jobsub_rm" % os.environ['POMS_DIR'], ""
             )

    '''
     def setoutput(self, fname):
         ###This one will fake the output
         print "using output " , fname
         os.environ['JOBSUB_RM_OUTPUT'] = fname
    '''
