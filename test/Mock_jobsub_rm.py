import os
import sys
import poms


class Mock_jobsub_rm:
    def __init__(self):
        ##Moving
        os.environ["JOBSUB_CLIENT_DIR"] = "%s/test/bin_mock_jobsub_rm" % (os.environ["POMS_DIR"])
        os.environ["PATH"] = "%s/test/bin_mock_jobsub_rm:%s" % (os.environ["POMS_DIR"], os.environ["PATH"])
        print(os.environ["PATH"])

    def close(self):
        os.environ["PATH"] = os.environ["PATH"].replace("%s/test/bin_mock_jobsub_rm" % os.environ["POMS_DIR"], "")
