
function split_type_picker() {
  ;
}

split_type_picker.custom_edit = function(split_type_id, dataset_id) {
    var e_sp, e_ds, current_val, current_type, parenpos;
    console.log(" custom_edit( " + split_type_id + "," + dataset_id )
    e_sp = document.getElementById(split_type_id)
    current_val = e_sp.value;
    parenpos = current_val.indexOf("(");
    if (parenpos > 0) {
        current_type = current_val.substring(0,parenpos);
    } else {
        current_type = current_val
    }
    console.log("checking for custom edit for " + current_type)
    if (split_type_edit_map[current_type]) {
       console.log("starting custom edit for " + current_type)
       split_type_edit_map[current_type].start(dataset_id)
    }
}

split_type_picker.start = function(id) {
    var rlist, form_id, sel_id,e, current_val, current_type, i, j;
    form_id = 'split_type_edit_' + id;
    sel_id = form_id + '_sel';
    rlist = [];
    e = document.getElementById(id);
    current_val = e.value;
    parenpos = current_val.indexOf("(");
    if (parenpos > 0) {
        current_type = current_val.substring(0,parenpos);
        current_params = current_val.substring(parenpos+1,current_val.length - 1);
        console.log("current params: " +  current_params)
        current_params = current_params.split(",");
        console.log(["current params:", current_params])
    } else { 
        current_type = current_val
        current_params = []
    }

    rlist.push('<h4>Split Type Chooser</h4>')
    rlist.push('<label>Base Split Type:</label>')
    rlist.push('<select id="' + sel_id + '" ' +
        'onchange="split_type_picker.showtype(' + "'" + form_id + "')" + '">');

    found=false
    for(split_type in split_type_param_map) {
       if (split_type == current_type) {
           found = true
       }
    }
    if (! found){
        split_type = 'None'
    }

    for(split_type in split_type_param_map) {
       if (split_type == current_type) {
          sel = " selected ";
       } else {
          sel = "";
       }
       rlist.push('<option value="' + split_type + '"' + sel + '>' + split_type + '</option>');
    }
    rlist.push('</select>');
    rlist.push('<input id="' + sel_id + '_sav" type="hidden" value="' + current_type + '">');
    for(split_type in split_type_param_map) {
       if (split_type == current_type) {
           hide = ' style="display: block;" ';
       } else {
           hide = ' style="display: none;" ';
       }
       rlist.push('<div id="' + form_id + '_' + split_type + '"'+ hide +'>');
       rlist.push('<div class="description">')
       rlist.push('<pre>')
       rlist.push(split_type_doc_map[split_type])
       rlist.push('</pre>')
       rlist.push('</div>')
       rlist.push('<div class="data">')
       rlist.push( '<h4>' + split_type + ' parameters:</h4>');
       if (split_type_param_map[split_type].length == 0) {
          rlist.push("<i>None</i>")
       }
       for (i in split_type_param_map[split_type]) {
           param = split_type_param_map[split_type][i]
           if ( param[param.length-1] == '=') {
               val = '';
               console.log("looking for " + param + " by name")
               /* match by name... */
               for ( j in current_params) {
                   cp = current_params[j]
                   console.log("checking: " + cp.substring(0,param.length+1))
                   if (cp.substring(0,param.length) == param) {
                       console.log("found: " + cp + " by name")
                       val = ' value="' + cp.substring(param.length) + '" ';
                    } 
               }
           } else {
               console.log("taking param", i)
               /* take the next one */
               val = ' value = "' + current_params[i] + '" '
           }
           rlist.push( param + ":");
           rlist.push('<input id="' + form_id + '_' + split_type + '_' + param + '"' + val + '>');
          rlist.push('<br>');
       }
       rlist.push('</div>');
       rlist.push('</div>');
    }
    rlist.push('<br>');
    rlist.push(`<button type="button" class="ui button deny red" onclick="split_type_picker.cancel('${form_id}')">Cancel</button>`);
    rlist.push(`<button type="button" class="ui button approve teal" onclick="split_type_picker.save('${form_id}')">Accept</button>`);

    var myform =  document.createElement("FORM");
    console.log(["got form", myform])
    myform.className = "popup_form_json";
    myform.style.top = '-10em';
    myform.style.left = '1em';
    myform.style.width = '50em';
    myform.style.position = 'absolute';
    myform.id = form_id;
    myform.innerHTML += rlist.join('\n');
    e.parentNode.appendChild(myform);
}

split_type_picker.showtype = function(form_id) {
    /* basically switch which one is hidden and update saved*/
    var e_sel, e_sav, e_div1, e_div2, split_type;
    e_sel = document.getElementById( form_id + '_sel')
    e_sav = document.getElementById( form_id + '_sel_sav')
    
    console.log("showtype - from " + e_sav.value + " to: " + e_sel.value )

    e_div1 = document.getElementById(form_id + '_' + e_sav.value)
    e_div2 = document.getElementById(form_id + '_' + e_sel.value)
    if (e_div1 && e_div2) {
        e_div1.style.display = 'none'
        e_div2.style.display = 'block'
    }
    e_sav.value = e_sel.value
}


split_type_picker.save = function(form_id) {
    var e_sal, split_type, param, e_inp, e_dest, id_inp, i;
    e_sal = document.getElementById( form_id + '_sel')
    split_type = e_sal.value
    res = split_type
    sep = '('
    for (i in split_type_param_map[split_type]) {
        param = split_type_param_map[split_type][i] 
        id_inp = form_id + '_' + split_type + '_' + param ;
        console.log("trying to get data from " + id_inp);
        e_inp = document.getElementById(id_inp);
        if (param[param.length-1] == '=') {
            res = res + sep + param + e_inp.value
        } else {
            res = res + sep + e_inp.value
        }
        sep = ','
    }
    if ( sep != '(') {
        res = res + ')'
    }
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


