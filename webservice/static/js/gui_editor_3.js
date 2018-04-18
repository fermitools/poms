"use strict";

/* utility function bundle */

function mwm_utils() {
   /* constructor does nothing... */
   return;
}

/* static functions for use elsewhere */

/* get url search parameters (i.e after "?" on url) */
mwm_utils.getSearchParams = function(){
 var p={};
 location.search.replace(/[?&]+([^=&]+)=([^&]*)/gi,function(s,k,v){p[k]=v});
 return p;
}

/* return count of pairs in dictionary */
mwm_utils.dict_size = function(d) {
   var c, i;
   c=0;
   for (i in d) {
     c++;
   }
   return c;
}

/* return list of keys in dictionary */
mwm_utils.dict_keys = function(d) {
   var res, i;
   res = [];
   for (i in d) {
     res.push(i);
   }
   return res;
}

/* return string without leading/traling blanks */
mwm_utils.trim_blanks = function (s) {
   var i, j;
   i = 0;
   if (s === undefined) {
      return '';
   }
   j = s.length-1;
   while(s[i]== ' '){
     i++;
   }
   while(s[j-1] == ' ') {
     j--;
   }
   return s.slice(i,j+1);
}

/* gui editor itself */

function gui_editor() {
    this.div = document.createElement("DIV");
    this.div.classname = 'gui_editor_frame';
    this.div.style.position='relative';
    this.div.addEventListener("dragover",gui_editor.dragover_handler);
    this.div.addEventListener("drop", gui_editor.drop_handler);
    this.div.style.width="100%";
    this.div.style.height="200em";
    this.stageboxes = [];
    this.miscboxes = [];
    this.depboxes = [];
    document.body.appendChild(this.div);
    gui_editor.instance_list.push(this);
}

/* static vars */

gui_editor.instance_list = []

/* static methods */

/* redraw all dependencies 'cause something moved */
gui_editor.redraw_all_deps = function() {
   console.log("redraw_all_deps: here")
   var i;
   for( i in gui_editor.instance_list ) {
       gui_editor.instance_list[i].redraw_deps()
   }
}

/* make form visible/invisible, save on invis */
gui_editor.toggle_form = function(id) {
    console.log("toggle_form: " + id)
    var e = document.getElementById(id)
    if (e && e.style.display == 'block') {
        console.log("hiding: " + id)
        if (e.parentNode && e.parentNode.gui_box) {
            console.log("trying to save values for " + id)
            e.parentNode.gui_box.save_values()
        }
        e.style.display = 'none'
    } else if ( e ) {
        console.log("unhiding: " + id)
        e.style.display = 'block'
        gui_editor.redraw_all_deps()
    }
}


gui_editor.toggle_box_selected = function(id) {
   var x = document.getElementById(id)
   if (x == null) {
      return;
   }
   if (x.className == 'box') {
       x.classList.add('selected')
   } else {
       x.classList.remove('selected')
   }
}

gui_editor.drag_start = function(ev) {
/* 
 * stash the element's id and the x,y coords inside the id in the text data 
 * of the event so we can drop it later
 */
   if (ev.target == null) {
       return;
   }
   var r = ev.target.getBoundingClientRect();
   var x = ev.x - r.x;
   var y = ev.y - r.y;
   ev.dataTransfer.setData("text",ev.target.id + "@" + x.toString() + "," + y.toString())
}

gui_editor.drop_handler = function(ev) {
    ev.preventDefault();
    console.log("drop_handler: ")
    var idatxy = ev.dataTransfer.getData("text")
    var idatxyl = idatxy.split(/[@,]/g)
    var id = idatxyl[0]
    var clickx = parseInt(idatxyl[1])
    var clicky = parseInt(idatxyl[2])
    var d = document.getElementById(id)
    var f = document.getElementById('fields_'+id)
    var r = d.parentNode.getBoundingClientRect();
    if (d != null) {
        d.style.left = (ev.x - clickx -r.x).toString() + "px"
        d.style.top = (ev.y - clicky - r.y).toString() + "px"
    }
    if (f != null) {
        f.style.left = (ev.x - clickx -r.x + 50).toString() + "px"
        f.style.top = (ev.y - clicky - r.y + 50).toString() + "px"
    }
    gui_editor.redraw_all_deps()
}

gui_editor.dragover_handler = function(ev) {
    ev.preventDefault();
    console.log("dragover_handler: ")
}

/* instance methods */

gui_editor.prototype.set_state = function (ini_dump) {
    this.state = JSON.parse(this.ini2json(ini_dump));
    this.draw_state()
}


gui_editor.prototype.un_trailing_comma = function(res) {
   // we end with: "foo": "bar",
   // and we're about to add a '}', 
   // so dink last comma.
   if (res.length > 0 && res[res.length-1].slice(-1) == ',') {
     res[res.length-1] = res[res.length-1].slice(0,-1);
   }
}

gui_editor.prototype.ini2json = function (s) {
   var res = [];
   var lines = s.split('\n');
   var l, k_v, k, v, i;
   for (i = 0 ; i < lines.length; i++) {
      if (lines[i] === undefined)
          break;
      l = mwm_utils.trim_blanks(lines[i]);

      // skip blank lines and comments
      if (l.length == 0)
          continue;
      if (l[0] == '#') {
          continue;

      } else if (l[0] == '[' &&  l[l.length-1]  == ']') {
          this.un_trailing_comma(res);
          res.push('},');
          res.push('"' + l.slice(1,-1) + '": {');
      } else {
          k_v = l.split('=');
          k = mwm_utils.trim_blanks(k_v.shift());
          v = mwm_utils.trim_blanks(k_v.join('=')).replace(/"/g,'\\"');
          if (k == "" || k[0] == " " || k[0] == "\n" || k[0] == '}') {
              continue;
          }
          res.push('"' + k + '": "' + v + '",');
      }
   }

   // fix leading line wart
   res[0] = '{';

   this.un_trailing_comma(res);
   res.push('}');
   res.push('}');
   return res.join('\n');
}

gui_editor.prototype.tsort = function (dlist) {
   var n, i, j, k,  t;
   n = dlist.length;
   console.log("tsort: start " + dlist.join(","))
   for(i = 0; i < n; i++) {
       for(j = 0; j < i; j++ ) {
           if ((j < i && this.checkdep(dlist[i],dlist[j]))) {
               t = dlist[i];
               dlist[i] = dlist[j];
               dlist[j] = t;
           } 
       }
   }
   console.log("tsort: finish " + dlist.join(","))
}

gui_editor.prototype.checkdep = function(s2, s1) {
   if (s2 == '') {
       return 0;
   }
   var k = "dependencies " + s1;

   if (!(k in this.state)) {
       return 0;
   }
   var deps = this.state[k];

   if( !('campaign_stage_1' in deps)) {
       return 0;
   }
   if (deps['campaign_stage_1'] == s2) {
       return 1
   }
   if( !( 'campaign_stage_2' in deps)) {
       return 0;
   }
   if (deps['campaign_stage_2'] == s2) {
       return 1
   }
   return 0
}

gui_editor.prototype.draw_state = function () {
   var grid = 200;
   var pad = 5;
   var x = pad;
   var y = pad;
   var i, prevstage,  istr, b;
   var stagelist, jobtypelist, launchtemplist, k;
   var db, istr;  
   prevstage = ""

   stagelist = []
   jobtypelist = []
   launchtemplist = []
 

   for (k in this.state) {
       if (k.startsWith('campaign_stage')) {
           stagelist.push(k.slice(15))
       } else if (k.startsWith('job_type')) {
           jobtypelist.push(k.slice(9))
       } else if (k.startsWith('launch_template')) {
           launchtemplist.push(k.slice(16))
       }
   }
         
   this.tsort(stagelist)

   for( i in stagelist) {
        k = 'campaign_stage ' + stagelist[i];
        console.log("cs: " + k + mwm_utils.dict_keys(this.state[k]).join(','));
        b = new stage_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y)
        this.stageboxes.push(b);
        /* wimpy layout, assumes tsorted list... */

        if (this.checkdep(prevstage, k.substr(15))) {
            y = y + grid;
        } else {
            x = x + grid;
        }
    }

    y = y + grid
    x = 0
    for (i in jobtypelist) {
        k = 'job_type ' + jobtypelist[i]
        b = new misc_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y)
        this.miscboxes.push(b)
        x = x + grid;
    }
    y = y + grid
    x = 0
    for (i in launchtemplist) {
        k = 'launch_template ' + launchtemplist[i];
        b = new misc_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y);
        this.miscboxes.push(b)
        x = x + grid;
    }
    for (k in this.state) {
        if (k.startsWith('dependencies')) {
          for (i = 1; i <= mwm_utils.dict_size(this.state[k])/2; i++) {
              istr = i.toString()
              db = new dependency_box(k+"_"+istr, this.state[k], ["campaign_stage_"+istr,"file_pattern_"+istr], this.div, x, y);
              this.depboxes.push(db)
          }
       }
   }
   y = y + 2 * grid;

   this.div.style.height=y.toString+"px"
}

gui_editor.prototype.redraw_deps = function () {
    var k;
    console.log("redraw_deps: here")
    for (k in this.depboxes) {
        this.depboxes[k].set_bounds()
    }
}


function generic_box(name, vdict, klist, top, x, y) {
    var i, k, x, y, val , placeholder;
    var stage, res;
    if (name == undefined) { 
       /* make prototype call work... */
       return;
    }
    this.dict = vdict;
    this.klist = klist;
    this.box = document.createElement("DIV");
    this.box.gui_box = this
    this.box.className = "box";
    this.box.id = name
    this.box.style.width = "120px";
    this.box.style.left = x.toString() + "px";
    this.box.style.top = y.toString() + "px";
    this.box.addEventListener("click", function(){gui_editor.toggle_box_selected(name)});
    this.popup_parent = document.createElement("DIV");
    this.popup_parent.gui_box = this
    this.popup_parent.className = "popup_parent";
    x = x+50;
    y = y+50;
    stage = name.substr(name.indexOf(" ")+1)
    res = [];
    res.push('<form id="fields_' + name + '" class="popup_form" style="display: none; top: '+ y.toString()+'px; left: ' +x.toString()+'px;">' );
    var val, placeholder;
    for ( i in klist ) {
       k = klist[i]
       if (vdict[k] == null) {
           val="";
           placeholder="default";
       } else {
           val=vdict[k];
           placeholder="";
       }
       res.push('<label>' + k + '</label> <input id="' + this.get_input_tag(k) + '" value="' + this.escape_quotes(val) + '" placeholder="'+placeholder+'"><br>');
    }
    res.push('</form>' );
    this.popup_parent.innerHTML = res.join('\n');
    top.appendChild(this.box);
    top.appendChild(this.popup_parent);
}

generic_box.prototype.get_input_tag = function(k) {

    return "_inp" + this.box.id + '_' + k
}

generic_box.prototype.save_values = function() {
    var inp_id, e, k, i; 
    for (i in this.klist) {
        k = this.klist[i]
        e = document.getElementById(this.get_input_tag(k))
        if (e != null) {
            this.dict[k] = e.value;
        } else {
            console.log('unable to find input ' + inp_id)
        }
    }
}

generic_box.prototype.escape_quotes = function(s) {
   if (s != undefined) {
       return s.replace(/"/g,'&quot;')
   } else { 
       return s
   }
}

function misc_box(name, vdict, klist, top, x, y) {
    this.generic_box = generic_box; /* superclass init */
    this.generic_box(name, vdict, klist, top, x, y);
    this.box.innerHTML = name + '<br> <button onclick="gui_editor.toggle_form(\'fields_' + name + '\')" id="wake_fields_' + name + '"><span class="wakefields"</span></button>' ;
}
misc_box.prototype = new generic_box()

function stage_box(name, vdict, klist, top, x, y) {
    this.generic_box = generic_box; /* superclass init */
    this.generic_box(name, vdict, klist, top, x, y);
    this.box.draggable = true;
    this.box.addEventListener("dragstart", gui_editor.drag_start)
    this.box.innerHTML = name + '<br> <button onclick="gui_editor.toggle_form(\'fields_' + name + '\')" id="wake_fields_' + name + '"><span class="wakefields"</span></button>' ;
}
stage_box.prototype = new generic_box()

function dependency_box(name, vdict, klist, top, x, y) {
    this.generic_box = generic_box;
    this.generic_box(name, vdict, klist, top, x, y) /* superclass init */;
    this.stage1 = mwm_utils.trim_blanks(vdict[klist[0]]);
    this.stage2 = mwm_utils.trim_blanks(name.slice(13, -2)); /* has a _1 or _2 on the end AND a 'campaign_stage ' on the front */
    /* this.box.id = 'dep_' + this.stage1 + '_' + this.stage2 */
    console.log("trying to do dependency from: " + this.stage1 + " to " + this.stage2)
    this.box.className = 'depbox';
    this.box.style.position='absolute'
    this.db = document.createElement("DIV");
    this.db.id = 'dep1_' + this.stage1 + '_' + this.stage2
    this.db.className = 'depbox1';
    this.box.appendChild(this.db);
    this.db2 = document.createElement("DIV")
    this.db2.className = 'depbuttonbox2'
    this.db2.style.position='absolute'
    top.appendChild(this.db2);
    this.db2.innerHTML = '<button onclick="gui_editor.toggle_form(\'fields_' + name + '\')" id="wake_fields_' + name + '"><span class="wakefields"</span></button>' ;

    this.set_bounds();
}
dependency_box.prototype = new generic_box()

dependency_box.prototype.set_bounds = function () {
   var e1 = document.getElementById("campaign_stage " + this.stage1);
   var e2 = document.getElementById("campaign_stage " + this.stage2);
   if ( e1 == null) {
      console.log("could not find campaign_stage: '" + this.stage1+ "'")
      return
   }
   if ( e2 == null) {
      console.log("could not find campaign_stage: '" + this.stage1 + "'")
      return
   }
   var br = e1.parentNode.getBoundingClientRect();
   var e1r = e1.getBoundingClientRect();
   var e2r = e2.getBoundingClientRect();
   var x1, x2, y1, y2, midx, midy, ulx, uly, lrx, lry, w, h;

   /* just go from center of one box to the other */
   x1 = (e1r.left + e1r.right) / 2
   x2 = (e2r.left + e2r.right) / 2
   y1 = (e1r.bottom  + e1r.top) / 2
   y2 = (e2r.top + e2r.bottom) / 2
   ulx = Math.min(x1, x2)
   lrx = Math.max(x1, x2)
   uly = Math.min(y1, y2)
   lry = Math.max(y1, y2)

   console.log("set_bounds(): "+ x1.toString() + "," + y1.toString() + ":" + x2.toString() + "," + y2.toString())

   var midx = (x1+x2)/2

   /* width and height... */
   w = lrx - ulx
   h = lry - uly

   var uphill = (y2 >= y1)

   /* make relative to bounding rectangle */
   ulx = ulx - br.x
   uly = uly - br.y
   lrx = lrx - br.x
   lry = lry - br.y
   midx = midx - br.x

   this.box.style.top = uly.toString()+"px"
   this.box.style.left = ulx.toString()+"px"
   this.box.style.height = h.toString() + "px"
   this.box.style.width = w.toString()+"px"

   if(  uphill ) {
       this.db.className = 'depbox1';
   } else {
       this.db.className = 'depbox1 uphill';
   }
   this.db2.style.left = (midx-10).toString()+"px"
   this.db2.style.top = lry.toString()+"px"
   var f = document.getElementById('fields_'+this.box.id)
   if (f != null) {
       f.style.left = (midx+10).toString()+"px"
       f.style.top = (lry+10).toString()+"px"
   } else {
      console.log("could not find: fields_" + this.box.id );
   }
}

