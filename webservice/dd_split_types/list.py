import poms.webservice.logit as logit
import ast
import re
from sqlalchemy import and_
from poms.webservice.poms_model import DataDispatcherSubmission
class list:
    """
       This split type assumes you have been given a comma-separated list 
       of dataset queries, or data dispatcher projects to work through in the dataset field, and will
       submit each one separately. Please follow the following format: ['files from namespace:name', 'project_id: INTEGER_VALUE']
    """

    def __init__(self, ctx, cs):
        self.cs = cs
        self.dmr_service = ctx.dmr_service
        self.list = ast.literal_eval(cs.data_dispatcher_dataset_query) if cs.data_dispatcher_dataset_query else []

    def params(self):
        return []

    def peek(self):
        if self.cs.cs_last_split is None:
            self.cs.cs_last_split = 0
        if self.cs.cs_last_split >= len(self.list):
            raise StopIteration
        
        query = self.list[self.cs.cs_last_split]
        dd_project = None
        project_name = "%s | list | item %d of %d" % (self.cs.name, self.cs.cs_last_split + 1, len(self.list))
        if "project_id:" in query:
            match = re.search(r'\b\d+\b', query)
            project_id = -1
            if match:
                project_id = int(match.group())
            else:
                raise ValueError("%s contains invalid project_id. Please format as 'project_id: INTEGER_VALUE'")
            if project_id > 0:
                dd_project = self.dmr_service.get_project_for_submission(project_id,
                                                                        username=self.cs.experimenter_creator_obj.username, 
                                                                        experiment=self.cs.experiment,
                                                                        role=self.cs.vo_role,
                                                                        project_name=project_name,
                                                                        campaign_id=self.cs.campaign_id, 
                                                                        campaign_stage_id=self.cs.campaign_stage_id,
                                                                        split_type=self.cs.cs_split_type,
                                                                        last_split=self.cs.cs_last_split,
                                                                        creator=self.cs.experimenter_creator_obj.experimenter_id,
                                                                        creator_name=self.cs.experimenter_creator_obj.username)
        else:
            project_files =  list(self.dmr_service.metacat_client.query(query, with_metadata=True))
            if len(project_files) == 0:
                raise StopIteration
            dd_project = self.dmr_service.create_project(username=self.cs.experimenter_creator_obj.username, 
                                                files=project_files,
                                                experiment=self.cs.experiment,
                                                role=self.cs.vo_role,
                                                project_name=project_name,
                                                campaign_id=self.cs.campaign_id, 
                                                campaign_stage_id=self.cs.campaign_stage_id,
                                                split_type=self.cs.cs_split_type,
                                                last_split=self.cs.cs_last_split,
                                                creator=self.cs.experimenter_creator_obj.experimenter_id,
                                                creator_name=self.cs.experimenter_creator_obj.username)
        if not dd_project:
            raise StopIteration
        
        return dd_project

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split + 1
        return res

    def prev(self):
        self.cs.cs_last_split = self.cs.cs_last_split - 1
        res = self.peek()
        return res

    def len(self):
        return len(self.list)

    def edit_popup(self):
        return """

        function list_edit_popup() {
             ;
        }
        list_edit_popup.start = function( id ) {
            var e, r, v ,res ,i, j, fid, ts;
            var hang_onto;
            e = document.getElementById(id);
            r = e.getBoundingClientRect();
            hang_onto = e.parentNode;
            fid = 'edit_form_' + id;
            res = [];
            ts = e.value.split(',');
            res.push('<textarea id="t1_' + fid +'">')
            for (i=0; i< ts.length; i++) {
                res.push(ts[i])
            }
            res.push('</textarea>')
            res.push('<button type="button" onclick="list_edit_popup.save('+"'"+id+"'"+')">Save</button>')
            res.push('<button type="button" onclick="list_edit_popup.cancel('+"'"+id+"'"+')">Cancel</button>')
            var myform = document.createElement("FORM")
            myform.className = "popup_form_json"
            myform.style.top = r.bottom
            myform.style.right = r.right
            myform.style.position = 'absolute'
            myform.id = fid
            myform.innerHTML += res.join('\\n');
            hang_onto.appendChild(myform)
        }
        list_edit_popup.save = function( id ) {
            var ta, e;
            console.log('in save('+id+'), starting...')
            ta = document.getElementById( 't1_edit_form_' + id )
            e = document.getElementById(id)
            console.log('in save, got: ' + e.value)
            console.log('in save, got: ' + ta.value)
            e.value = ta.value.split('\\n').join(',')
            list_edit_popup.cancel(id)
        }
        list_edit_popup.cancel = function( id ) {
            var e;
            e = document.getElementById('edit_form_' + id)
            e.parentNode.removeChild(e)
        }

       """
