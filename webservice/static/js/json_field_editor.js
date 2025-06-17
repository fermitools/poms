/* constructor, not used since everything is static.. */

function json_field_editor() {
    ;
}

json_field_editor.recovery_start = function (id, dd=true) {
    var e, e_text, r, i, j, k, si;
    var hang_onto, recoveries;

    console.log("recovery_start(" + id + ")")

    const sam_recoveries = {
        '-': '',
        'added_files': 'Include files added to definition since previous job ran',
        'consumed_status': 'Include files from the total dataset which were not flagged "consumed" by the original job',
        'delivered_not_consumed': 'Include only delivered files which were not "consumed" by the original job',
        'pending_files': 'Include files from total dataset which do not have suitable children declared for this version of software',
        'process_status': 'Like consumed status, but also include files from that were processed by jobs that say they failed',
    }

    const dd_recoveries = {
        '-': '',
        'added_files': 'Include files added to definition since previous job ran',
        'state_failed': 'Reset files that failed during processing, and resubmit.',
        'state_not_done': 'Reset files with a reserved or failed state, and resubmit.',
        'reprocess_orphans': 'Reset files that have completed with a "done" state, but did not produce children.',
        'reprocess_all': 'Reset and resubmit the entire project.'
    }
    const dd_match = {
        'consumed_status': 'state_not_done',
        'process_status': 'state_failed'
    }

    recoveries = dd ? dd_recoveries: sam_recoveries;

    e = document.getElementById(id);
    e_text = document.getElementById(id + '_text');
    if (e_text) {
        r = e_text.getBoundingClientRect();
        hang_onto = e_text.parentNode;
    } else {
        r = e.getBoundingClientRect();
        hang_onto = e.parentNode;
    }
    v = e.value || e.placeholder;
    if ('' == v || '[]' == v || '"[]"' == v) {
        j = [];
    } else {
        j = JSON.parse(v);
    }
    fid = 'edit_recovery_' + id;
    res = [];
    res.push('<h4>Edit Recoveries</h4>');
    res.push('<table>');
    res.push('<tr><th>Type</th><th>Param Overrides</th><th></th></tr>');
    for (i = 0; i < 10; i++) {
        si = i.toString();
        res.push('<tr>');
        res.push('<td><select id="' + fid + '_s' + si + '">');
        for (k in recoveries) {
            console.log(j, i, 0)
            const rtype = (i < j.length) ? (dd ? (j[i][0] in dd_match ? dd_match[j[i][0]]:j[i][0]): j[i][0]) : k;
            if (i < j.length && rtype == k) {
                res.push('<option value="' + k + '" selected>' + k + ' - ' + recoveries[k] + '</option>');
            }
            else {
                res.push('<option value="' + k + '">' + k + ' - ' + recoveries[k] + '</option>');
            }
        }
        res.push('</select></td>');
        res.push('<td>');
        if (i < j.length) {
            res.push('<input id="' + fid + '_d' + si + '" value=' + "'" + JSON.stringify(j[i][1]) + "'>");
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
    var myform = document.createElement("FORM");
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

json_field_editor.recovery_save = function (fid) {
    /*
     * extract values from the form back in to destination input
     */
    var j, e, i, si, sid, did, se, de, saveid, savee, dest;
    if (! json_field_editor.validate_percent_formats(fid)) {
       return;
    }
    j = [];
    for (i = 0; i < 10; i++) {
        si = i.toString();
        sid = fid + '_s' + si;
        did = fid + '_d' + si;
        se = document.getElementById(sid);
        de = document.getElementById(did);
        console.log("got sid " + sid + " value " + se.value);
        console.log("got did " + did + " value " + de.value);
        if (se.value != '' && se.value != '-') {
            j.push([se.value, JSON.parse(de.value)]);
        }
    }
    saveid = fid.substr(14);
    console.log("updating saveid " + saveid);
    savee = document.getElementById(saveid);
    savee.value = JSON.stringify(j);
    dest = document.getElementById(saveid + '_text');
    if (dest) {
        dest.value = JSON.stringify(j);
        console.log(["also updating", saveid + '_text', dest, j]);
    }
    json_field_editor.cancel(fid);
}

json_field_editor.dictstart = function (id, name=null) {
    var e, r, v, res, i, j, fid, istr, k, e_text;
    var hang_onto;
    e = document.getElementById(id);
    e_text = document.getElementById(id + '_text');
    if (e_text) {
        r = e_text.getBoundingClientRect();
        hang_onto = e_text.parentNode;
    } else {
        r = e.getBoundingClientRect();
        hang_onto = e.parentNode;
    }
    v = e.value || e.placeholder;
    if ('' == v || '[]' == v || '{}' == v || '"[]"' == v) {
        j = {}
    } else {
        j = JSON.parse(v);
    }
    count = 0
    for( k in j ) {
        count = count + 1
    }
    
    fid = 'edit_form_' + id;
    res = [];
    res.push('<input type="hidden" id="' + fid + '_count" value="' + count.toString() + '">');
    res.push(`<h3 style="margin-top: 0">${name? name: 'Keyword Editor'}</h3>`);
    res.push('<table style="border-spacing: 5px; border-collapse: separate; border: 1px solid gray;">');
    res.push('<thead>');
    res.push('<tr>');
    res.push('<td>Key <a target="_blank" href="https://github.com/fermitools/poms/wiki/Campaign-Edit-Help#Key"><i class="icon help circle link"></i></a></td>');
    res.push('<td>Value <a htarget="_blank" ref="https://github.com/fermitools/poms/wiki/Campaign-Edit-Help#Value"><i class="icon help circle link"></i></a></td>');
    res.push('<td>&nbsp;</td>');
    res.push('</tr>');
    res.push('</thead>');
    res.push('<tbody id="' + fid + '_tbody">');
    i = 0
    for (k in j) {
        v = j[k]
        res.push('<tr>');
        json_field_editor.addrow(res, fid, i, k, v,false);
        res.push('</tr>');
        i = i + 1
    }
    if(i == 0) {
        res.push('<tr><td>')
        res.push('<i onclick="json_field_editor.plus(\'' + fid + '\',' + istr + ', ' + ')" class="blue icon dlink plus square"></i>');
        res.push('</td></tr>')
    }
    res.push('</tbody>');
    res.push('</table>');
    res.push('&nbsp;&nbsp;&nbsp;');
    res.push(`<button type="button" class="ui button deny red" onclick="json_field_editor.cancel('${fid}')">Cancel</button>`);
    res.push(`<button type="button" class="ui button approve teal" onclick="json_field_editor.dictsave('${fid}')">Accept</button>`);
    var myform = document.createElement("FORM");
    myform.className = "popup_form_json";
    myform.style.top = r.bottom;
    myform.style.right = r.right;
    myform.style.position = 'absolute';
    myform.id = fid;
    myform.innerHTML += res.join('\n');
    // hang_onto.style.position = 'relative';
    hang_onto.appendChild(myform);
}
json_field_editor.start = function (id) {
    var e, r, v, res, i, j, fid, istr, k, e_text;
    var hang_onto;
    e = document.getElementById(id);
    e_text = document.getElementById(id + '_text');
    if (e_text) {
        r = e_text.getBoundingClientRect();
        hang_onto = e_text.parentNode;
    } else {
        r = e.getBoundingClientRect();
        hang_onto = e.parentNode;
    }
    v = e.value || e.placeholder;
    if ('' == v || '[]' == v  || '"[]"' == v) {
        j = [
            ['', '']
        ];
    } else {
        j = JSON.parse(v);
    }
    fid = 'edit_form_' + id;
    res = [];
    res.push('<input type="hidden" id="' + fid + '_count" value="' + j.length.toString() + '">');
    res.push('<h3 style="margin-top: 0">Param Editor</h3>');
    res.push('<table style="border-spacing: 5px; border-collapse: separate; borer: 1px solid gray;">');
    res.push('<thead>');
    res.push('<tr>');
    res.push('<td>Key <a target="_blank" href="https://github.com/fermitools/poms/wiki/Campaign-Edit-Help#Key"><i class="icon help circle link"></i></a></td>');
    res.push('<td align="center">Space<a target="_blank" href="https://github.com/fermitools/poms/wiki/Campaign-Edit-Help#Space"><i class="icon help circle link"></i></a></td>');
    res.push('<td>Value <a htarget="_blank" ref="https://github.com/fermitools/poms/wiki/Campaign-Edit-Help#Value"><i class="icon help circle link"></i></a></td>');
    res.push('<td>&nbsp;</td>');
    res.push('</tr>');
    res.push('</thead>');
    res.push('<tbody id="' + fid + '_tbody">');
    for (i in j) {
        k = j[i][0];
        v = j[i][1];
        res.push('<tr>');
        json_field_editor.addrow(res, fid, i, k, v,true);
        res.push('</tr>');
    }
    res.push('</tbody>');
    res.push('</table>');
    res.push('&nbsp;&nbsp;&nbsp;');
    res.push(`<button type="button" class="ui button deny red" onclick="json_field_editor.cancel('${fid}')">Cancel</button>`);
    res.push(`<button type="button" class="ui button approve teal" onclick="json_field_editor.save('${fid}')">Accept</button>`);
    var myform = document.createElement("FORM");
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
json_field_editor.addrow = function (res, fid, i, k, v, blanks) {
    var istr = i.toString(),
        ws, wsr;
    var blanks_symbol;
    if (blanks) {
        if (k[k.length - 1] == ' ') {
            while (k[k.length - 1] == ' ') {
                k = k.slice(0, -1);
            }
            ws = 'checked="true"';
        } else {
            ws = '';
        }
        if (v[0] == ' ') {
            while (v[0] == ' ') {
                v = v.slice(1);
            }
            wsr = 'checked="true"'
        } else {
            wsr = '';
        }
    }
    res.push('<td><input id="' + fid + '_k_' + istr + '" value="' + k + '"></td>');
    if (blanks) {
        blanks_symbol = 'true';
        res.push('<td style="min-width: 5em;"><input style="padding: auto; width: 2em;" type="checkbox" id="' + fid + '_ws_' + istr + '" ' + ws + ' value=" ">');
        res.push('<input style="padding: auto; width: 2em;" type="checkbox" id="' + fid + '_wsr_' + istr + '" ' + wsr + ' value=" "></td>');
    } else {
        blanks_symbol = 'false';
    }

    res.push('<td><input id="' + fid + '_v_' + istr + '" value="' + v + '"></td>');
    res.push('<td style="min-width: 7em;">');
    res.push('<i onclick="json_field_editor.plus(\'' + fid + '\',' + istr + ', ' + blanks_symbol + ')" class="blue icon dlink plus square"></i>');
    res.push('<i onclick="json_field_editor.minus(\'' + fid + '\',' + istr + ', ' + blanks_symbol + ')" class="blue icon dlink minus square"></i>');
    res.push('<i onclick="json_field_editor.up(\'' + fid + '\',' + istr + ', ' + blanks_symbol + ')" class="blue icon dlink arrow square up"></i>');
    res.push('<i onclick="json_field_editor.down(\'' + fid + '\',' + istr + ', ' + blanks_symbol + ')" class="blue icon dlink arrow square down"></i>');
}

json_field_editor.renumber = function (fid, c, blanks) {
    var i;
    var res;
    var blanks_symbol;
    var tb = document.getElementById(fid + '_tbody');
    for (i = 0; i < c; i++) {
        istr = i.toString();
        tr = tb.children[i];
        tr.children[0].children[0].id = fid + "_k_" + istr;
        if (blanks) {
            blanks_symbol = 'true';
            tr.children[1].children[0].id = fid + "_ws_" + istr;
            tr.children[1].children[1].id = fid + "_wsr_" + istr;
            tr.children[2].children[0].id = fid + "_v_" + istr;
        } else {
            blanks_symbol = 'false';
            tr.children[1].children[0].id = fid + "_v_" + istr;
        }
        res = [];
        res.push('<i onclick="json_field_editor.plus(\'' + fid + '\',' + istr + ', ' + blanks_symbol + ')" class="blue icon dlink plus square"></i>');
        res.push('<i onclick="json_field_editor.minus(\'' + fid + '\',' + istr + ', ' + blanks_symbol + ')" class="blue icon dlink minus square"></i>');
        res.push('<i onclick="json_field_editor.up(\'' + fid + '\',' + istr + ', ' + blanks_symbol + ')" class="blue icon dlink arrow square up"></i>');
        res.push('<i onclick="json_field_editor.down(\'' + fid + '\',' + istr + ', ' + blanks_symbol + ')" class="blue icon dlink arrow square down"></i>');
        if (blanks) {
            tr.children[3].innerHTML = res.join("\n");
        } else {
            tr.children[2].innerHTML = res.join("\n");
        }
    }
}

json_field_editor.plus = function (fid, i, blanks) {
    var res = [];
    var tb = document.getElementById(fid + '_tbody');
    var tr = document.createElement('TR');
    var ce = document.getElementById(fid + '_count');
    console.log("before: count: " + ce.value);
    var c = parseInt(ce.value);

    json_field_editor.addrow(res, fid, c, "", "", blanks);
    tr.innerHTML = res.join("\n");

    ce.value = (c + 1).toString();
    console.log("after: count: " + ce.value);

    tb.insertBefore(tr, tb.children[i]);
    json_field_editor.renumber(fid, c + 1, blanks);
}

json_field_editor.minus = function (fid, i, blanks) {
    var tb = document.getElementById(fid + '_tbody');
    tb.removeChild(tb.children[i]);
    var c = parseInt(document.getElementById(fid + '_count').value);
    document.getElementById(fid + '_count').value = (c - 1).toString();
    json_field_editor.renumber(fid, c - 1, blanks);
}

json_field_editor.up = function (fid, i, blanks) {
    var tb = document.getElementById(fid + '_tbody');
    var c = parseInt(document.getElementById(fid + '_count').value);
    if (i == 0) {
        return
    }
    var tr = tb.children[i];
    tb.removeChild(tr);
    tb.insertBefore(tr, tb.children[i - 1])
    json_field_editor.renumber(fid, c, blanks);
}

json_field_editor.down = function (fid, i, blanks) {
    var tb = document.getElementById(fid + '_tbody');
    var c = parseInt(document.getElementById(fid + '_count').value);
    if (i == c-1) {
       return;
    }
    var tr = tb.children[i+1];
    tb.removeChild(tr);
    tb.insertBefore(tr, tb.children[i]);
    json_field_editor.renumber(fid, c, blanks);
}

json_field_editor.dictsave = function (fid) {
    /*
     * extract values from the form back in to destination input
     */
    var e, ke, ve, res, id, dest, i, istr;
    var ws, wsr;
    if (! json_field_editor.validate_percent_formats(fid)) {
       return;
    }
    e = document.getElementById(fid);
    var ce = document.getElementById(fid + '_count');
    console.log("before: count: " + ce.value);
    var c = parseInt(ce.value);

    res = {};
    console.log(["count", c]);
    for (i = 0; i < c; i++) {
        istr = i.toString();
        ke = document.getElementById(fid + '_k_' + istr);
        ve = document.getElementById(fid + '_v_' + istr);
        if (ke != null && ve != null) {
            res[ke.value] = ve.value
            console.log(["adding", ke.value, ve.value]);
        } else {
            console.log(['cannot find:', fid + 'k' + istr, fid + 'v' + istr]);
        }
    }
    id = fid.replace('edit_form_', '');
    dest = document.getElementById(id);
    dest.value = JSON.stringify(res).replace(/","/g,'", "');      // To comply with Python json.dumps() format
    /* also update xxx_text if there is one */
    console.log(["updating", id, dest, res]);
    dest = document.getElementById(id + '_text');
    if (dest) {
        dest.value = JSON.stringify(res).replace(/","/g,'", "');      // To comply with Python json.dumps() format
        console.log(["also updating", id + '_text', dest, res]);
    }
    json_field_editor.cancel(fid);
}

json_field_editor.save = function (fid) {
    /*
     * extract values from the form back in to destination input
     */
    var e, ke, ve, res, id, dest, i, istr;
    var ws, wsr;
    if (! json_field_editor.validate_percent_formats(fid)) {
       return;
    }
    e = document.getElementById(fid);
    var ce = document.getElementById(fid + '_count');
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
        if (ke != null && ve != null && ws != null && wsr != null) {
            var pair = [ke.value + (ws.checked ? " " : ""), (wsr.checked ? " " : "") + ve.value];
            res.push(pair);
            console.log(["adding", pair]);
        } else {
            console.log(['cannot find:', fid + 'k' + istr, fid + 'v' + istr]);
        }
    }
    id = fid.replace('edit_form_', '');
    dest = document.getElementById(id);
    dest.value = JSON.stringify(res).replace(/","/g,'", "');      // To comply with Python json.dumps() format
    /* also update xxx_text if there is one */
    console.log(["updating", id, dest, res]);
    dest = document.getElementById(id + '_text');
    if (dest) {
        dest.value = JSON.stringify(res).replace(/","/g,'", "');  // To comply with Python json.dumps() format
        console.log(["also updating", id+'_text', dest, res]);
    }
    json_field_editor.cancel(fid);
}

json_field_editor.cancel = function (fid) {
    /*
     * delete the form
     */
    var e = document.getElementById(fid);
    e.parentNode.removeChild(e);
}

json_field_editor.validate_percent_formats = function(form) {
   var el,e,s,i,j,k,word;
   e = document.getElementById(form);
   el = e.elements
   for(i = 0; i < el.length; i++) {
       e = el[i]
       /* file patterns are not % replaced, and have % wildcards... */
       if (e.id.indexOf("file_pattern") >= 0) {
          continue;
       }
       json_field_editor.validate_percent_ok(e);
       s = e.value
       if (! s ) { 
           continue;
       }
       k = s.indexOf('%');
       while (k >= 0) {
           if (s[k+1] == '%') {
              /* %% is okay */
              k = s.indexOf('%',k+2);
              continue;
           }
           if (s[k+1] != '(') {
               /* paren of %(word)s */
               return json_field_editor.validate_percent_error(e, "missing '('");
           }
           l = s.indexOf(')',k);
           if (l < 0) {
               /* thesis of %(word)s */
               return json_field_editor.validate_percent_error(e, "missing ')'");
           }
           if (s[l+1] != 's') {
               /* s of %(word)s */
               return json_field_editor.validate_percent_error(e, "missing 's'");
           }
           word = s.substring(k+2,l);
           if (! (word in { "dataset": 1, "parameter": 1, "experiment": 1, "version": 1, "group": 1, "experimenter":1}) && !(typeof document.extra_keywords !== "undefined" && word in document.extra_keywords)) {
               return json_field_editor.validate_percent_error(e, "unknown keyword '" + word + "'");
           }
           k = s.indexOf('%',k+1);
       }
   }
   return true
}

json_field_editor.validate_percent_error = function( e, msg ) {
    var le, ep;
    /* mark the node with error class, 
    ** make the preceding label have a sub-label with class=error
    ** with our message
    */
    msg = "Error in %(keyword)s format: " + msg + "<br>"
    e.classList.add('error')
    le = document.createElement("LABEL")
    le.style.minWidth = "14em";
    le.className = 'error'
    le.innerHTML = msg
    ep = e.parentNode
    ep.insertBefore(le, ep.firstChild)
    return false
}

json_field_editor.validate_percent_ok = function( e ) {
    /* pretty much undo error stuff from above */
    var ep;
    e.classList.remove('error')
    ep = e.parentNode
    if (ep && ep.firstElementChild && ep.firstElementChild != undefined) {
        if( ep.firstElementChild.className == 'error') {
            ep.removeChild(ep.firstElementChild);
        }
    }
}
