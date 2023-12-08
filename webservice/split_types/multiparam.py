import json
import uuid


class multiparam:
    """
       This split type assumes you have been given a list of lists of 
       strings, and returns a string of the nth combination ... for
       example if you have 3 lists
           +-----+-----+-----+
           | a   | d   | g   |
           | b   | e   | h   |
           | c   | f   | i   |
           +-----+-----+-----+
        this should give you the 27 permutations:
        a_d_g
        b_d_g
        c_d_g
        a_e_g
        ...
        c_f_i
        
        This split_tpe has a custom editor for the list-of-lists dataset value
    """

    def __init__(self, cs, samhandle, dbhandle, test=False):
        self.test = test
        self.cs = cs
        self.list = json.loads(cs.dataset)
        self.dims = []
        self.id = uuid.uuid4()
        if self.list:
            for l1 in self.list:
                self.dims.append(len(l1))

    def params(self):
        return []

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

        return "%s" % ("_".join(res))

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split + 1
        return res

    def prev(self):
        self.cs.cs_last_split = self.cs.cs_last_split - 1
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
            if (e.value[0] == '[') {
                ts = JSON.parse(e.value)
            } else {
                ts = [ [], []]
            }
            res.push('<div>')
            res.push('<h4>Multiparam Editor </h4>')
            res.push('<input type="hidden" id="edit_ncolumns_' + id + '" value="' + ts.length.toString() + '">');
            for (i=0; i< ts.length; i++) {
                res.push('<textarea id="t' + i.toString() + '_' + fid +'">')
                for(j = 0; j < ts[i].length; j++) {
                    res.push(ts[i][j])
                }
                res.push('</textarea>')
            }
            res.push('</div>')
            res.push('<button type="button" onclick="multiparam_edit_popup.add_column('+ "'" +id+ "'" + ')">Add Column</button>')
            // we need a way to add more columns....
            res.push('<button type="button" onclick="multiparam_edit_popup.save('+ "'" +id+ "'" + ')">Save</button>')
            res.push('<button type="button" onclick="multiparam_edit_popup.cancel('+ "'" +id+ "'" + ')">Cancel</button>')
            var myform = document.createElement("FORM")
            myform.className = "popup_form_json"
            myform.style.top = r.bottom
            myform.style.right = r.right
            myform.style.width = '30em'
            myform.style.position = 'absolute'
            myform.id = fid
            myform.innerHTML += res.join('\\n');
            hang_onto.appendChild(myform)
        }
        multiparam_edit_popup.add_column = function( id ) {
            var nce, ncols, fid, fe;
            nce = document.getElementById('edit_ncolumns_' + id)
            ncols = parseInt(nce.value, 10)
            fid = 'edit_form_' + id;
            fe = document.getElementById(fid)
            te = document.createElement("TEXTAREA")
            te.id = 't' + ncols.toString() + '_' + fid
            ncols = ncols + 1;
            nce.value = ncols.toString()
            fe.firstElementChild.appendChild(te)
        }
        multiparam_edit_popup.save = function( id ) {
            var ta, e;
            console.log('in save('+id+'), starting...')
            nce = document.getElementById('edit_ncolumns_' + id)
            ncols = parseInt(nce.value, 10)
            res = []
            for (i = 0; i< ncols ; i++ ) {
                console.log("fetching box " + i.toString() )
                ta = document.getElementById( 't'+i.toString()+'_edit_form_' + id )
                if ( ta ) {
                    res.push(ta.value.split('\\n'))
                }
            }
            e = document.getElementById(id)
            e.value = JSON.stringify(res)
            multiparam_edit_popup.cancel(id)
        }
        multiparam_edit_popup.add_col = function( id ) {
            nce = document.getElementById('edit_ncolumns_' + id)
            ncols = nce.value
            newbox = document.createElement("TEXTAREA")
            newbox.id = 't' + ncols.toString() + '_editform_' + id
            nce.parentNode.push(newbox)
            nce.value = ncols + 1
        }
        multiparam_edit_popup.cancel = function( id ) {
            var e;
            e = document.getElementById('edit_form_' + id)
            e.parentNode.removeChild(e)
        }

    """
