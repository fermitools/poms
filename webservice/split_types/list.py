
class list:
    """
       This split type assumes you have been given a comma-separated list 
       of dataset names to work through in the dataset field, and will
       submit each one separately
    """
    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.list = cs.dataset.split(',')

    def peek(self):
        if self.cs.cs_last_split == None:
            self.cs.cs_last_split = 0
        if self.cs.cs_last_split >= len(self.list):
            raise StopIteration
        return self.list[self.cs.cs_last_split]

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split+1
        return res

    def prev(self):
        self.cs.cs_last_split = self.cs.cs_last_split-1
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
            res.push('<button type="button" onclick="list_edit_popup.save(\''+id+'\')">Save</button>')
            res.push('<button type="button" onclick="list_edit_popup.cancel(\''+id+'\')">Cancel</button>')
            var myform = document.createElement("FORM")
            myform.className = "popup_form_json"
            myform.style.top = r.bottom
            myform.style.right = r.right
            myform.style.position = 'absolute'
            myform.id = fid
            myform.innerHTML += res.join('\n');
            hang_onto.appendChild(myform)
        }
        list_edit_popup.save = function( id ) {
            var ta, e;
            console.log('in save('+id+'), starting...')
            ta = document.getElementById( 't1_edit_form_' + id )
            e = document.getElementById(id)
            console.log('in save, got: ' + e.value)
            console.log('in save, got: ' + ta.value)
            e.value = ta.value.split('\n').join(',')
            list_edit_popup.cancel(id)
        }
        list_edit_popup.cancel = function( id ) {
            var e;
            e = document.getElementById('edit_form_' + id)
            e.parentNode.removeChild(e)
        }

       """
