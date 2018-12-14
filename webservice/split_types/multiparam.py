import json

class multiparam:
    """
       This split type assumes you have been given a list of lists of 
       strings, and returns a string of the nth combination ... for
       example if you have:
        [['a','b','c'],['d','e','f'],['g','h','i']]
        this should give you:
        a_d_g
        b_d_g
        c_d_g
        a_e_g
        b_e_g
        ...
    """
    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.list = json.loads(cs.dataset)
        self.dims = []
        if self.list:
            for l1 in self.list:
                self.dims.append(len(l1))

    def peek(self):
        if self.cs.cs_last_split == None:
            self.cs.cs_last_split = 0
        if self.cs.cs_last_split >= self.len():
            raise StopIteration
        n = self.cs.cs_last_split 
        res = []
        i = 0
        for l1 in self.list:
            res.append(l1[n % self.dims[i]])
            n = n // self.dims[i]
            i = i + 1

        return '_'.join(res)

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split+1
        return res

    def prev(self):
        self.cs.cs_last_split = self.cs.cs_last_split-1
        res = self.peek()
        return res

    def len(self):
        c = 1
        for l1 in self.list:
            c = c * len(l1)
        return c

    def edit_popup(self):
        return """


        function multiparam_edit_popup() {
             ;
        }
        multiparam_edit_popup.start = function( id ) {
            var e, r, v ,res ,i, j, fid, ts;
            var hang_onto;
            e = document.getElementById(id);
            r = e.getBoundingClientRect();
            hang_onto = e.parentNode;
            fid = 'edit_form_' + id;
            res = [];
            ts = JSON.parse(e.value)
            res.push('<input type="hidden" id="edit_ncolumns'+id+'" value="'+str(ts.length)+'">')
            for (i=0; i< ts.length; i++) {
                res.push('<textarea id="t' + str(i) + '_' + fid +'">')
                for(j = 0; j < ts[i].length; j++) {
                    res.push(ts[i][j])
                }
                res.push('</textarea>')
            }
            // we need a way to add more columns....
            res.push('<button type="button" onclick="multiparam_edit_popup.save(\''+id+'\')">Save</button>')
            res.push('<button type="button" onclick="multiparam_edit_popup.cancel(\''+id+'\')">Cancel</button>')
            var myform = document.createElement("FORM")
            myform.className = "popup_form_json"
            myform.style.top = r.bottom
            myform.style.right = r.right
            myform.style.position = 'absolute'
            myform.id = fid
            myform.innerHTML += res.join('\n');
            hang_onto.appendChild(myform)
        }
        multiparam_edit_popup.save = function( id ) {
            var ta, e;
            console.log('in save('+id+'), starting...')
            nce = document.getElementById('edit_ncolumns_' + id)
            ncols = nce.value
            res = []
            for (i = 0; i< ncols ; i++ ) {
                ta = document.getElementById( 't'+str(i)+'_edit_form_' + id )
                res.append(ta.value.split(\n))
            }
            e = document.getElementById(id)
            e.value = JSON.stringify(res)
            multiparam_edit_popup.cancel(id)
        }
        multiparam_edit_popup.add_col = function( id ) {
            nce = document.getElementById('edit_ncolumns_' + id)
            ncols = nce.value
            newbox = document.createElement("TEXTAREA")
            newbox.id = 't' + str(ncols) + '_editform_' + id
            nce.parentNode.appendChild(newbox)
            nce.value = ncols + 1
        }
        multiparam_edit_popup.cancel = function( id ) {
            var e;
            e = document.getElementById('edit_form_' + id)
            e.parentNode.removeChild(e)
        }

    """
