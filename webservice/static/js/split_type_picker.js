
function split_type_picker() {
  ;
}

split_type_picker.start = function(id) {
    var rlist, form_id, sel_id,e, current_val, current_type;
    form_id = 'split_type_edit_' + id;
    sel_id = form_id + '_sel';
    rlist = [];
    e = document.getElementById(id);
    current_val = e.value;
    parenpos = current_val.indexOf("(");
    current_type = current_val.substring(0,parenpos);
    current_params = current_val.substring(parenpos+1,current_val.length - 2);
    current_params = current_val.split(",");

    rlist.push('<select id="' + sel_id + '" ' +
        'onselect="split_type_picker.showtype(' + "'" + form_id + "')" + '">');
    for(split_type in split_type_param_map) {
       if (split_type == current_type) {
          sel = " selected ";
       } else {
          sel = "";
       }
       rlist.push('<option value="'+split_type+'"'+sel+'>'+split_type+'</option>');
    }
    rlist.push('</select>');
    rlist.push('<input id="' + sel_id + '_sav" type="hidden" value="' + current_type + '">');
    for(split_type in split_type_param_map) {
       if (split_type == current_type) {
           hide = ' style="display: none;" ';
       } else {
           hide = ' style="display: block;" ';
       }
       rlist.push('<div id="' form_id + '_' + split_type + '"'+ hide +'>');
       rlist.push( '<h4>' + split_type + ' parameters:</h4>');
       for (param in split_type_param_map[split_type]) {
           val = '';
           for ( cp in current_parameters) {
               if (cp.substring(0,param.length+1) == param+'=') {
                   val = 'value="' + cp.substring(param.length+1) + '" ';
                } 
           }
           rlist.push( param + ":");
           rlist.push('<input id="' form_id + '_' + split_type + '_' + param + '"' + val + '>');
          rlist.push('<br>');
       }
       rlist.push('</div>');
    }
    rlist.push(`<button type="button" class="ui button deny red" onclick="split_type_picker.cancel('${form_id}')">Cancel</button>`);
    rlist.push(`<button type="button" class="ui button approve teal" onclick="json
_field_editor.save('${form_id}')">Accept</button>`);

    var myform =  document.createElement("FORM");
    myform.className = "popup_form_json";
    myform.style.top = '-10em';
    myform.style.left = '1em';
    myform.style.width = '50em';
    myform.style.position = 'absolute';
    myform.id = fid;
    myform.innerHTML += rlist.join('\n');
}

split_type_picker.showtype = function(form_id) {
    /* basically switch which one is hidden and update saved*/
    var e_sel, e_sav, e_div1, e_div2, split_type;
    e_sal = document.getElementById( form_id + '_sel')
    e_sav = document.getElementById( form_id + '_sel_sav')
    
    e_div1 = document.getElementById(form_id + '_' + e_sav.value)
    e_div2 = document.getElementById(form_id + '_' + e_sel.value)
    e_div1.style.display = 'hidden'
    e_div2.style.display = 'block'
    e_sav.value = e_sel.value
}

split_type_picker.save = function(form_id) {
    var e_sal, split_type, param, e_inp, e_dest;
    e_sal = document.getElementById( form_id + '_sel')
    split_type = e_sal.value
    res = split_type
    sep = '('
    for (param in split_type_param_map[split_type]) {
        e_inp = getElementById(
        if (param[param.length-1] == '=') {
            res = res + sep + param + e_inp.value
        } else {
            res = res + sep + e_inp.value
        }
        sep = ','
    }
    res = res + ')'
    e_dest = document.getElementById(form_id.substr(16))
    e_dest.value = res
    split_type_picker.cancel(form_id)
}

split_type_picker.cancel = function(form_id) {
 /*
  * delete the form
  */
    var e = document.getElementById(form_id);
    e.parentNode.removeChild(e)
}

