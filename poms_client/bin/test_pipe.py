#!/usr/bin/env python
'''def my_function(**kwargs):
    print "inicio"
    print kwargs
    print "the arguments are: \n"
    for kw in kwargs.items():
        print kw
D = {'primero':1, 'segundo':2, 'tercero':3}
#D = {primero=1, segundo=2, tercero=3}

#print "print D", D
#make_poms_call(primero == 1.0, segundo == 2, tercero ==3, cuarto == 4)
print "calling with the list"
my_function(D)
#my_function(primero = '1', segundo='2', tercero='3')'''


def launch_template_edit_test(**kwargs):
    print "---*4"
    action = kwargs.pop('action',None)
    name = kwargs.pop('name',None)
    launch_host = kwargs.pop('launch_host',None)
    user_account = kwargs.pop('user_account',None)
    launch_setup = kwargs.pop('launch_setup',None)
    experiment = kwargs.pop('experiment',None)
    pc_email = kwargs.pop('pc_email',None)
    print 'action =', action
    print 'name =', name
    print 'launch_host=',launch_host
    print 'user_account=',user_account
    print "launch_setup =",launch_setup
    print "experiment=", experiment
    print "pc_email=", pc_email
    #for k in kwargs.items():
        #print k
    #for k,v in kwargs.items():
    #    print k,v


def campaign_definition_edit_test(**kwargs):
    print "---*4"
    action = kwargs.pop('action')
    name = kwargs.pop('name')
    email= kwargs.pop('pc_email')
    experiment = kwargs.pop('experiment')
    input_files_per_job = kwargs.pop('input_files_per_job')
    output_files_per_job = kwargs.pop('output_files_per_job')
    output_file_patterns = kwargs.pop('output_file_patterns')
    launch_script = kwargs.pop('launch_script')
    def_parameter =kwargs.pop('def_parameter')
    print 'The arguments you pass are:'
    print 'action =', action
    print 'name =', name
    print 'email=', email
    print 'experiment=', experiment
    print 'input_files_per_job=', input_files_per_job
    print 'output_files_per_job=', output_files_per_job
    print 'output_file_patterns=', output_file_patterns
    print 'launch_script=', launch_script
    print 'def_parameter=', def_parameter


def campaign_edit_test(**kwargs):
    print "\n---*4"
    action=kwargs.pop('action')
    ae_campaign_name=kwargs.pop('ae_campaign_name')
    pc_email=kwargs.pop('pc_email')
    experiment=kwargs.pop('experiment')
    vo_role=kwargs.pop('vo_role')
    dataset=kwargs.pop('dataset')
    ae_active=kwargs.pop('ae_active')
    ae_split_type=kwargs.pop('ae_split_type')
    ae_software_version=kwargs.pop('ae_software_version')
    ae_completion_type=kwargs.pop('ae_completion_type')
    ae_completion_pct=kwargs.pop('ae_completion_pct')
    ae_param_overrides=kwargs.pop('ae_param_overrides')
    ae_depends=kwargs.pop('ae_depends')
    ae_launch_name=kwargs.pop('ae_launch_name')
    ae_campaign_definition=kwargs.pop('ae_campaign_definition')
    test_client=kwargs.pop('test_client')
    print "printing Arguments \n"
    print 'action=', action
    print 'ae_campaign_name=', ae_campaign_name
    print 'pc_email=', pc_email
    print 'experiment=', experiment
    print 'vo_role=', vo_role
    print 'dataset=', dataset
    print 'ae_active=', ae_active
    print 'ae_split_type=', ae_split_type
    print 'ae_software_version=', ae_software_version
    print 'ae_completion_type=', ae_completion_type
    print 'ae_completion_pct=', ae_completion_pct
    print 'ae_param_overrides=', ae_param_overrides
    print 'ae_depends=', ae_depends
    print 'ae_launch_name=', ae_launch_name
    print 'ae_campaign_definition=', ae_campaign_definition
    print 'test_client=', test_client



'''
def launch_template_edit_test(action = None, name = None, launch_host = None, user_account = None, launch_setup = None, experiment = None, pc_email=None):
    var_test = action
    print "------- my variable is: ", var_test
'''

'''
def my_prueba_uno(var3=None):
    if var3 in [None,""]:
        print "var3",var3
    else:
        print "the variables were"+str(var3)

def my_prueba(var1=None,var2=None):
    if (var1 or var2) in [None,""]:
        print "var1",var1
        print "var2",var2
    else:
        print "the variables were "+str(var1)+" "+str(var2)
var1=5
var2=None
my_prueba(var1,var2)
print "mi prueba uno"
var3=None
my_prueba_uno(var3)
var3=""
my_prueba_uno(var3)
'''
