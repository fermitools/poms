
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
    res.push('<td>Value <a href="https://cdcvs.fnal.gov/redmine/projects/prod_mgmt_db/wiki/CampaignEditHelp#Value" class="helpbutton">?</a></td>');
    res.push('<td>&nbsp;</td>');
    res.push('</tr>');
    res.push('</thead>');
    res.push('<tbody>');
    for (i in j) {
        k = j[i][0];
        v = j[i][1];
        json_field_editor.addrow(res, fid, i, k, v);
    }
    res.push('</tbody>');
    res.push('</table>');
    res.push('<button type="button" onclick="json_field_editor.cancel(\''+fid+'\')" >Cancel</button>');
    res.push('<button type="button" onclick="json_field_editor.save(\''+fid+'\')" >Accept</button>');
    var myform =  document.createElement("FORM");
    myform.className = "popup_form"
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
        res.push('<tr>');
        res.push('<td><input id="'+fid+'k'+istr+'" value="'+k+'"></td>');
        res.push('<td><input id="'+fid+'v'+istr+'" value="'+v+'"></td>');
        res.push('<td>');
        res.push('<button type="button" onclick="json_field_editor.plus(\''+ fid+'\','+istr+')" >+</button>');
        res.push('<button type="button" onclick="json_field_editor.minus(\''+ fid+'\','+istr+')" >-</button>');
        res.push('<button type="button" onclick="json_field_editor.up(\''+ fid+'\','+istr+')" >↑</button>');
        res.push('<button type="button" onclick="json_field_editor.down(\''+ fid+'\','+istr+')" >↓</button>');
        res.push('</tr>');
}

json_field_editor.plus= function(fid,i) {
  alert('not implemented')
}
json_field_editor.minus= function(fid,i) {
  alert('not implemented')
}
json_field_editor.up = function(fid,i) {
  alert('not implemented')
}
json_field_editor.down = function(fid,i) {
  alert('not implemented')
}
json_field_editor.save = function(fid) {
    /*
     * extract values from the form back in to destination input
     */
    var e,c, ke, ve, res, id, dest, i, istr;
    e = document.getElementById(fid);
    c = parseInt(document.getElementById(fid+'_count').value);
    res = [];
    console.log(["count", c])
    for (i = 0 ; i < c ; i++ ) {
        istr = i.toString();
        ke = document.getElementById(fid + 'k' + istr);
        ve = document.getElementById(fid + 'v' + istr);
        if (ke != null && ve != null) {
            var pair=[ ke.value, ve.value ]
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
