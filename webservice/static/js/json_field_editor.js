
/* constructor, not used since everything is static.. */

function json_field_editor() {
  ;
}
json_field_editor.start = function(id) {
    var e, r,v, res, i, j, fid, istr, k;
    e = document.getElementById(id);
    r = e.getBoundingClientRect();
    v = e.value;
    if ('' == v || '[]' == v) {
        j = [['','']]
    } else {
        j = JSON.parse(v);
    }
    fid = 'edit_form_'+id;
    res = [];
    res.push('<input type="hidden" id="'+fid+'_count" value="'+j.length.toString()+'">');
    res.push('<table>');
    res.push('<thead>');
    res.push('<tr>');
    res.push('<td>Key <a href="https://cdcvs.fnal.gov/redmine/projects/prod_mgmt_db/wiki/CampaignEditHelp#Key" class="helpbutton">?</a></td>');
    res.push('<td>&nbsp;<a href="https://cdcvs.fnal.gov/redmine/projects/prod_mgmt_db/wiki/CampaignEditHelp#Space" class="helpbutton">?</a></td>');
    res.push('<td>Value <a href="https://cdcvs.fnal.gov/redmine/projects/prod_mgmt_db/wiki/CampaignEditHelp#Value" class="helpbutton">?</a></td>');
    res.push('<td>&nbsp;</td>');
    res.push('</tr>');
    res.push('</thead>');
    res.push('<tbody id="'+fid+'_tbody">');
    for (i in j) {
        k = j[i][0];
        v = j[i][1];
        res.push('<tr>');
        json_field_editor.addrow(res, fid, i, k, v);
        res.push('</tr>');
    }
    res.push('</tbody>');
    res.push('</table>');
    res.push('<button type="button" onclick="json_field_editor.cancel(\''+fid+'\')" >Cancel</button>');
    res.push('<button type="button" onclick="json_field_editor.save(\''+fid+'\')" >Accept</button>');
    var myform =  document.createElement("FORM");
    myform.className = "popup_form_json "
    myform.style.top = r.bottom
    myform.style.right = r.right
    myform.id = fid
    myform.innerHTML += res.join('\n');
    gui_editor.body.appendChild(myform)
}

/*
 * add a row to the popup editor.  This is factored out so
 * the plus-button callback can share it..
 */
json_field_editor.addrow= function(res, fid, i, k, v) {
        var istr = i.toString();
        if (k[k.length-1] == ' ' || v[0] == ' ') {
            while( k[k.length-1] == '') {
               k = k.slice(0,-1)
            }
            while( v[0] == '') {
               v = v.slice(1)
            }
            ws = 'checked="true"'
        } else {
            ws = ''
        }
        res.push('<td><input id="'+fid+'_k_'+istr+'" value="'+k+'"></td>');
        res.push('<td><input type="checkbox" id="'+fid+'_ws_'+istr+'" '+ws+' value=" "></td>')

        res.push('<td><input id="'+fid+'_v_'+istr+'" value="'+v+'"></td>');
        res.push('<td>');
        res.push('<button type="button" onclick="json_field_editor.plus(\''+ fid+'\','+istr+')" >+</button>');
        res.push('<button type="button" onclick="json_field_editor.minus(\''+ fid+'\','+istr+')" >-</button>');
        res.push('<button type="button" onclick="json_field_editor.up(\''+ fid+'\','+istr+')" >↑</button>');
        res.push('<button type="button" onclick="json_field_editor.down(\''+ fid+'\','+istr+')" >↓</button>');
}
json_field_editor.renumber= function(fid,c) {
    var i;
    var tb = document.getElementById(fid+'_tbody');
    for(i = 0; i< c; i++ ) {
        istr = i.toString()
        tr = tb.children[i]
        tr.children[0].children[0].id = fid+"_k_"+istr
        tr.children[1].children[0].id = fid+"_ws_"+istr
        tr.children[2].children[0].id = fid+"_v_"+istr
        tr.children[3].children[0].addEventListener("click",function(){json.field_editor.plus(fid,i)})
        tr.children[3].children[1].addEventListener("click",function(){json.field_editor.minus(fid,i)})
        tr.children[3].children[2].addEventListener("click",function(){json.field_editor.up(fid,i)})
        tr.children[3].children[3].addEventListener("click",function(){json.field_editor.down(fid,i)})
    }
}

json_field_editor.plus= function(fid,i) {
    var res = []     
    var tb = document.getElementById(fid+'_tbody');
    var tr = document.createElement('TR');
    var ce = document.getElementById(fid+'_count')
    console.log("before: count: " + ce.value)
    var c = parseInt(ce.value);
    
    json_field_editor.addrow(res, fid, c, "", "");
    tr.innerHTML = res.join("\n")

    ce.value = (c + 1).toString();
    console.log("after: count: " + ce.value)

    tb.insertBefore(tr, tb.children[i])
    json_field_editor.renumber(fid,c+1);
}

json_field_editor.minus= function(fid,i) {
    var tb = document.getElementById(fid+'_tbody');
    tb.removeChild(tb.children[i]);
    var c = parseInt(document.getElementById(fid+'_count').value);
    document.getElementById(fid+'_count').value = (c - 1).toString();
    json_field_editor.renumber(fid,c);
}

json_field_editor.up = function(fid,i) {
    var tb = document.getElementById(fid+'_tbody');
    var tr = tb.children[i];
    var c = parseInt(document.getElementById(fid+'_count').value);
    tb.removeChild(tr);
    tb.insertBefore(tr, tb.children[i-1])
    json_field_editor.renumber(fid,c);
}

json_field_editor.down = function(fid,i) {
    var tb = document.getElementById(fid+'_tbody');
    var tr = tb.children[i];
    var c = parseInt(document.getElementById(fid+'_count').value);
    tb.removeChild(tr);
    tb.insertBefore(tr, tb.children[i+1])
    json_field_editor.renumber(fid,c);
}

json_field_editor.save = function(fid) {
    /*
     * extract values from the form back in to destination input
     */
    var e, ke, ve, res, id, dest, i, istr;
    e = document.getElementById(fid);
    var ce = document.getElementById(fid+'_count')
    console.log("before: count: " + ce.value)
    var c = parseInt(ce.value);
    
    res = [];
    console.log(["count", c])
    for (i = 0 ; i < c ; i++ ) {
        istr = i.toString();
        ke = document.getElementById(fid + '_k_' + istr);
        ws = document.getElementById(fid + '_ws_' + istr);
        ve = document.getElementById(fid + '_v_' + istr);
        if (ke != null && ve != null) {
            var pair=[ ke.value + ws.value, ve.value ]
            res.push(pair);
            console.log(["adding", pair])
        } else {
            console.log(['cannot find:', fid+'k'+istr, fid+'v'+istr]);
        }
    }
    id = fid.replace('edit_form_','')
    dest = document.getElementById(id)
    dest.value = JSON.stringify(res)
    console.log(["updating", id, dest, res])
    json_field_editor.cancel(fid)
}
json_field_editor.cancel= function(fid) {
    /*
     * delete the form
     */
    var e = document.getElementById(fid);
    gui_editor.body.removeChild(e)
}
