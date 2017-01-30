import os
import sys

class MockCondor_q:
     def __init__(self):
         os.environ['PATH'] = "%s/test/bin_mock_condor_q:%s" % ( 
                os.environ['POMS_DIR'],os.environ['PATH']
             )

     def close(self):
         os.environ['PATH'] = os.environ['PATH'].replace( 
                 "%s/test/bin_mock_condor_q:" % os.environ['POMS_DIR'], ""
             )

     def setoutput(self, fname):
         print "using output " , fname
         os.environ['CONDOR_Q_OUTPUT'] = fname

