# -- faked out bits for launch_jobs, until we get rid of 
#    them...

def gethead(h,d):
    return None

class fake_e:
    def __init__(self):
        self.email = 'mengel@fnal.gov'
        self.username = 'mengel'

    def is_authorized_for(self,exp):
        return True

    def is_authorized(self,exp):
        return True


class mock_seshandle:
    def __call__(self,x):
        return fake_e()

    def get(self,x,y = None):
        if x == 'experimenter':
           return fake_e()
        return None

    def __setitem__(self,x,y):
        return 

def getconfig(x, y=None):
    if x == 'poms.launch_recovery_jobs':
        return True
    return None

camp_seshandle = mock_seshandle()
launch_seshandle = mock_seshandle()

err_res = "200 Ok."
