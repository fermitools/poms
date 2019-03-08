
/* constructor, not used since everything is static.. */

function json_field_editor() {
  ;
}

json_field_editor.recovery_start = function(id) {
    var e, e_text, r, i, j, k, si;
    var hang_onto, recoveries;

    console.log("recovery_start(" +id +")" )

    recoveries = {
            '-': '',
            'added_files': 'Include files added to definition since previous job ran',
            'consumed_status': 'Include files which were not flagged "consumed" by the original job',
            'pending_files': 'Include files which do not have suitable children declared for this version of software',
            'proj_status': 'Include files that were processed by jobs that say they failed'
    }
    e = document.getElementById(id);
    e_text = document.getElementById(id+'_text');
    if (e_text) {
        r = e_text.getBoundingClientRect();
        hang_onto = e_text.parentNode;
    } else {
        r = e.getBoundingClientRect();
        hang_onto = e.parentNode;
    }
    v = e.value || e.placeholder;
    if ('' == v || '[]' == v) {
        j = [];
    } else {
        j = JSON.parse(v);
    }
    fid = 'edit_recovery_' + id;
    res = [];
    res.push('<h4>Edit Recoveries</h4>');
    res.push('<table>');
    res.push('<tr><th>Type</th><th>Param Overrides</th><th></th></tr>');
    for ( i = 0; i < 10 ; i++) {
        si = i.toString();
        res.push('<tr>');
        res.push('<td><select id="'+fid+'_s'+si+'">');
        for (k in recoveries) {

            if (i < j.length && j[i][0] == k) {
                res.push('<option value="' + k + '" selected>' + k + ' - ' + recoveries[k] +'</option>');
            } else {
                res.push('<option value="' + k + '">'+ k + ' - '  + recoveries[k] + '</option>');
            }
        }
        res.push('</select></td>');
        res.push('<td>');
        if ( i < j.length) {
            res.push('<input id="' + fid + '_d' + si + '" value='+ "'" + JSON.stringify(j[i][1]) + "'>");
        } else {
            res.push('<input id="' + fid + '_d' + si + '">');
        }
        res.push('<i class="large edit link blue icon" onclick="json_field_editor.start(' + "'" + fid + '_d' + si + "'" + ')"></i>');
        res.push('</td>');
        res.push('</tr>');
    }
    res.push('</table>');
    res.push(`<button type="button" class="ui button deny red" onclick="json_field_editor.cancel('${fid}')">Cancel</button>`);
    res.push(`<button type="button" class="ui button approve teal" onclick="json_field_editor.recovery_save('${fid}')">Accept</button>`);
    var myform =  document.createElement("FORM");
    myform.className = "popup_form_json";
    myform.style.top = '-10em';
    myform.style.left = '1em';
    myform.style.width = '50em';
    myform.style.position = 'absolute';
    myform.id = fid;
    myform.innerHTML += res.join('\n');
    // hang_onto.style.position = 'relative';
    hang_onto.appendChild(myform);
}

json_field_editor.recovery_save = function(fid) {
    /*
     * extract values from the form back in to destination input
     */
    var j, e, i, si, sid, did, se, de, saveid, savee, dest;
    j = [];
    for ( i = 0; i < 10 ; i++) {
        si = i.toString();
        sid = fid + '_s' + si;
        did = fid + '_d' + si;
        se = document.getElementById(sid);
        de = document.getElementById(did);
        console.log("got sid " + sid + " value " + se.value);
        console.log("got did " + did + " value " + de.value);
        if (se.value != '' && se.value != '-') {
           j.push([se.value,JSON.parse(de.value)]);
        }
    }
    saveid = fid.substr(14);
    console.log("updating saveid " + saveid);
    savee = document.getElementById(saveid);
    savee.value = JSON.stringify(j);
    dest = document.getElementById(saveid+'_text');
    if (dest) {
        dest.value = JSON.stringify(j);
        console.log(["also updating", saveid+'_text', dest, j]);
    }
    json_field_editor.cancel(fid);
}

json_field_editor.start = function(id) {
    var e, r,v, res, i, j, fid, istr, k, e_text;
    var hang_onto;
    e = document.getElementById(id);
    e_text = document.getElementById(id+'_text');
    if (e_text) {
        r = e_text.getBoundingClientRect();
        hang_onto = e_text.parentNode;
    } else {
        r = e.getBoundingClientRect();
        hang_onto = e.parentNode;
    }
    v = e.value || e.placeholder;
    if ('' == v || '[]' == v) {
        j = [['', '']];
    } else {
        j = JSON.parse(v);
    }
    fid = 'edit_form_'+id;
    res = [];
    res.push('<input type="hidden" id="'+fid+'_count" value="'+j.length.toString()+'">');
    res.push('<h3 style="margin-top: 0">Param Editor</h3>');
    res.push('<table style="border-spacing: 5px; border-collapse: separate; borer: 1px solid gray;">');
    res.push('<thead>');
    res.push('<tr>');
    res.push('<td>Key <a target="_blank" href="https://cdcvs.fnal.gov/redmine/projects/prod_mgmt_db/wiki/CampaignEditHelp#Key"><i class="icon help circle link"></i></a></td>');
    res.push('<td align="center">Space<a target="_blank" href="https://cdcvs.fnal.gov/redmine/projects/prod_mgmt_db/wiki/CampaignEditHelp#Space"><i class="icon help circle link"></i></a></td>');
    res.push('<td>Value <a htarget="_blank" ref="https://cdcvs.fnal.gov/redmine/projects/prod_mgmt_db/wiki/CampaignEditHelp#Value"><i class="icon help circle link"></i></a></td>');
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
    res.push('&nbsp;&nbsp;&nbsp;');
    res.push(`<button type="button" class="ui button deny red" onclick="json_field_editor.cancel('${fid}')">Cancel</button>`);
    res.push(`<button type="button" class="ui button approve teal" onclick="json_field_editor.save('${fid}')">Accept</button>`);
    var myform =  document.createElement("FORM");
    myform.className = "popup_form_json";
    myform.style.top = r.bottom;
    myform.style.right = r.right;
    myform.style.position = 'absolute';
    myform.id = fid;
    myform.innerHTML += res.join('\n');
    // hang_onto.style.position = 'relative';
    hang_onto.appendChild(myform);
}

/*
 * add a row to the popup editor.  This is factored out so
 * the plus-button callback can share it..
 */
json_field_editor.addrow = function(res, fid, i, k, v) {
        var istr = i.toString(), ws, wsr;
        if (k[k.length-1] == ' ' ) {
            while( k[k.length-1] == ' ') {
               k = k.slice(0,-1);
            }
            ws = 'checked="true"';
        } else {
            ws = '';
        }
        if (v[0] == ' ') {
            while( v[0] == ' ') {
               v = v.slice(1);
            }
            wsr = 'checked="true"'
        } else {
            wsr = '';
        }
        res.push('<td><input id="'+fid+'_k_'+istr+'" value="'+k+'"></td>');
        res.push('<td><input style="padding: auto; width: 2em;" type="checkbox" id="'+fid+'_ws_'+istr+'" '+ws+' value=" ">');
        res.push('<input style="padding: auto; width: 2em;" type="checkbox" id="'+fid+'_wsr_'+istr+'" '+wsr+' value=" "></td>');

        res.push('<td><input id="'+fid+'_v_'+istr+'" value="'+v+'"></td>');
        res.push('<td>');
        res.push('<i onclick="json_field_editor.plus(\''+ fid+'\','+istr+')" class="blue icon dlink plus square"></i>');
        res.push('<i onclick="json_field_editor.minus(\''+ fid+'\','+istr+')" class="blue icon dlink minus square"></i>');
        res.push('<i onclick="json_field_editor.up(\''+ fid+'\','+istr+')" class="blue icon dlink arrow square up"></i>');
        res.push('<i onclick="json_field_editor.down(\''+ fid+'\','+istr+')" class="blue icon dlink arrow square down"></i>');
}

json_field_editor.renumber = function(fid,c) {
    var i;
    var res;
    var tb = document.getElementById(fid+'_tbody');
    for(i = 0; i < c; i++ ) {
        istr = i.toString();
        tr = tb.children[i];
        tr.children[0].children[0].id = fid + "_k_" + istr;
        tr.children[1].children[0].id = fid + "_ws_" + istr;
        tr.children[1].children[1].id = fid + "_wsr_" + istr;
        tr.children[2].children[0].id = fid + "_v_" + istr;
        res = [];
        res.push('<i onclick="json_field_editor.plus(\''+ fid+'\','+istr+')" class="blue icon dlink plus square"></i>');
        res.push('<i onclick="json_field_editor.minus(\''+ fid+'\','+istr+')" class="blue icon dlink minus square"></i>');
        res.push('<i onclick="json_field_editor.up(\''+ fid+'\','+istr+')" class="blue icon dlink arrow square up"></i>');
        res.push('<i onclick="json_field_editor.down(\''+ fid+'\','+istr+')" class="blue icon dlink arrow square down"></i>');
        tr.children[3].innerHTML = res.join("\n");
    }
}

json_field_editor.plus = function(fid,i) {
    var res = [];
    var tb = document.getElementById(fid+'_tbody');
    var tr = document.createElement('TR');
    var ce = document.getElementById(fid+'_count');
    console.log("before: count: " + ce.value);
    var c = parseInt(ce.value);

    json_field_editor.addrow(res, fid, c, "", "");
    tr.innerHTML = res.join("\n");

    ce.value = (c + 1).toString();
    console.log("after: count: " + ce.value);

    tb.insertBefore(tr, tb.children[i]);
    json_field_editor.renumber(fid,c+1);
}

json_field_editor.minus = function(fid,i) {
    var tb = document.getElementById(fid+'_tbody');
    tb.removeChild(tb.children[i]);
    var c = parseInt(document.getElementById(fid+'_count').value);
    document.getElementById(fid+'_count').value = (c - 1).toString();
    json_field_editor.renumber(fid, c - 1);
}

json_field_editor.up = function (fid, i) {
    var tb = document.getElementById(fid + '_tbody');
    var tr = tb.children[i];
    var c = parseInt(document.getElementById(fid + '_count').value);
    tb.removeChild(tr);
    tb.insertBefore(tr, tb.children[i - 1])
    json_field_editor.renumber(fid, c);
}

json_field_editor.down = function(fid,i) {
    var tb = document.getElementById(fid+'_tbody');
    var tr = tb.children[i];
    var c = parseInt(document.getElementById(fid+'_count').value);
    tb.removeChild(tr);
    tb.insertBefore(tr, tb.children[i+1]);
    json_field_editor.renumber(fid,c);
}

json_field_editor.save = function(fid) {
    /*
     * extract values from the form back in to destination input
     */
    var e, ke, ve, res, id, dest, i, istr;
    var ws, wsr;
    e = document.getElementById(fid);
    var ce = document.getElementById(fid+'_count');
    console.log("before: count: " + ce.value);
    var c = parseInt(ce.value);

    res = [];
    console.log(["count", c]);
    for (i = 0; i < c; i++) {
        istr = i.toString();
        ke = document.getElementById(fid + '_k_' + istr);
        ws = document.getElementById(fid + '_ws_' + istr);
        wsr = document.getElementById(fid + '_wsr_' + istr);
        ve = document.getElementById(fid + '_v_' + istr);
        if (ke != null && ve != null) {
            var pair = [ke.value + (ws.checked ? " " : ""), (wsr.checked ? " " : "") + ve.value];
            res.push(pair);
            console.log(["adding", pair]);
        } else {
            console.log(['cannot find:', fid + 'k' + istr, fid + 'v' + istr]);
        }
    }
    id = fid.replace('edit_form_', '');
    dest = document.getElementById(id);
    dest.value = JSON.stringify(res).replace(/([,])/g,'$1 ');      // To comply with Python json.dumps() format
    /* also update xxx_text if there is one */
    console.log(["updating", id, dest, res]);
    dest = document.getElementById(id + '_text');
    if (dest) {
        dest.value = JSON.stringify(res).replace(/([,])/g,'$1 ');  // To comply with Python json.dumps() format
        console.log(["also updating", id+'_text', dest, res]);
    }
    json_field_editor.cancel(fid);
}

json_field_editor.cancel = function(fid) {
    /*
     * delete the form
     */
    var e = document.getElementById(fid);
    e.parentNode.removeChild(e);
}
