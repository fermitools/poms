/* utility function bundle */

mwm_utils = function() {
   return;
}

mwm_utils.getSearchParams = function(){
 var p={};
 location.search.replace(/[?&]+([^=&]+)=([^&]*)/gi,function(s,k,v){p[k]=v})
 return p;
}

mwm_utils.dict_size = function(d) {
   c=0
   for (i in d) {
     c++
   }
   return c
}

mwm_utils.dict_keys = function(d) {
   var res, i
   res = []
   for (i in d) {
     res.push(i)
   }
   return res
}

mwm_utils.trim_blanks = function (s) {
   var i, j;
   i = 0;
   if (s === undefined) {
      return '';
   }
   j = s.length;
   while(s[i]== ' '){
     i++;
   }
   while(s[j-1] == ' ') {
     j--;
   }
   return s.slice(i,j);
}

/* gui editor itself */

gui_editor = function () {
    this.div = document.createElement("DIV");
    this.div.classname = 'gui_editor_frame';
    this.div.style.position='relative';
    this.div.addEventListener("drop", gui_editor.drop_handler)
    this.div.addEventListener("dragover",gui_editor.dragover_handler)
    this.div.style.width="1024px"
    this.div.style.height="1024px"
    this.div.style.backgroundColor="#ccddbb"
    this.stageboxes = []
    this.miscboxes = []
    this.depboxes = []
    document.body.appendChild(this.div);
    gui_editor.instance_list.push(this)
}

/* static vars */

gui_editor.instance_list = []

/* static methods */

gui_editor.redraw_all_deps = function() {
   var i;
   for( i in gui_editor.instance_list ) {
       gui_editor.instance_list[i].redraw_deps()
   }
}



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
   var r = ev.target.getBoundingClientRect();
   var x = ev.x - r.x;
   var y = ev.y - r.y;
   ev.dataTransfer.setData("text",ev.target.id + "@" + x.toString() + "," + y.toString())
}

gui_editor.drop_handler = function(ev) {
    ev.preventDefault();
    var idatxy = ev.dataTransfer.getData("text")
    var idatxyl = idatxy.split(/[@,]/g)
    console.log("drop_handler: " + idatxy)
    var id = idatxyl[0]
    var clickx = parseInt(idatxyl[1])
    var clicky = parseInt(idatxyl[2])
    var d = document.getElementById(id)
    var r = d.parentNode.getBoundingClientRect();
    if (d != null) {
        d.style.left = (ev.x - clickx -r.x).toString() + "px"
        d.style.top = (ev.y - clicky - r.y).toString() + "px"
    }
}

gui_editor.dragover_handler = function(ev) {
    console.log("dragover_handler: ")
    ev.preventDefault();
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

gui_editor.prototype.checkdep = function(s1, prevstage) {
   if (prevstage == '') {
       return 0;
   }
   deps = self.state["dependencies " + s1]
   if ( deps == undefined || deps['campaign_stage_1'] != prevdep) {
       return 1
   } else {
       return 0
   }
}

gui_editor.prototype.draw_state = function () {
   var grid = 200;
   var pad = 5;
   var x = pad;
   var y = pad;
   var i, prevstage, prevtype, istr, b;
   prevtype = ""
   prevstage = ""

   for (k in this.state) {
       if ((k == 'campaign') || ( k == 'global')) {
           ;
       } else if (k.startsWith('campaign_stage')) {
           b = new stage_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y)
           this.stageboxes.push(b)
           /* wimpy layout, assumes tsorted list... */

           if (this.checkdep(k.substr(15), prevstage)) {
               y = y + grid;
           } else {
               x = x + grid;
           }
       } else if (k.startsWith('launch_template')) {
           b = new misc_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y);
           this.miscboxes.push(b)
           if (prevtype != 'launch_template') {
               y = y + grid;
               x = pad;
           }
           prevtype = 'launch_template';
       } else if (k.startsWith('job_type')) {
           b = new misc_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y)
           this.miscboxes.push(b)
           if (prevtype != 'job_type') {
               y = y + grid;
               x = pad;
           }
           prevtype = 'launch_template';
       } else if (k.startsWith('dependencies')) {
          for (i = 1; i <= mwm_utils.dict_size(this.state[k])/2; i++) {
              var istr = i.toString()
              db = new dependency_box(k+istr, this.state[k], ["campaign_stage_"+istr,"file_pattern_"+istr], this.div, x, y);
              this.depboxes.push(db)
          }
       } else {
           alert("unknown item " + k);
       }
   }
}

gui_editor.prototype.redraw_deps = function () {
    for (k in this.depboxes) {
        this.depboxes[k].set_bounds()
    }
}

generic_box = function (name, vdict, klist, top, x, y) {
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
    this.box.onclick = "gui_editor.toggle_box_selected(name)";
    this.popup_parent = document.createElement("DIV");
    this.popup_parent.gui_box = this
    this.popup_parent.className = "popup_parent";
    x = x+50;
    y = y+50;
    stage = name.substr(name.indexOf(" ")+1)
    res = [];
    res.push('<form id="fields_' + name + '" class="popup_form" style="display: none; top: '+ y.toString()+'px; left: ' +x.toString()+'px;">' );
    res.push('<input id="_tag" type="hidden" value="%s">' % stage);
    var val, placeholder;
    for ( k in klist ) {
       if (vdict[k] == null) {
           val="";
           placeholder="default";
       } else {
           val=stage_dict[k];
           placeholder="";
       }
       res.push('<label>' + k + '</label> <input id="_inp_' + name + '_' + k + '" value="' + this.escape_quotes(val) + '" placeholder="'+placeholder+'"><br>');
    }
    res.push('</form>' );
    this.popup_parent.innerHTML = res.join('\n');
    top.appendChild(this.box);
    top.appendChild(this.popup_parent);
}

generic_box.prototype.save_values = function() {
    var inp_id, e
    for (k in this.klist) {
        inp_id = "_inp_" + this.box.id + '_' + k;
        e = document.getElementById(inp_id)
        this.dict[k] = e.value;
    }
}

generic_box.prototype.escape_quotes = function(s) {
   if (s != undefined) {
       return s.replace(/"/g,'&quot;')
   } else { 
       return s
   }
}

misc_box  = function(name, vdict, klist, top, x, y) {
    this.generic_box = generic_box; /* superclass init */
    this.generic_box(name, vdict, klist, top, x, y);
    this.box.innerHTML = name + '<br> <button onclick="gui_editor.toggle_form(\'fields_' + name + '\')" id="wake_fields_' + name + '"><span class="wakefields"</span></button>' ;
}
misc_box.prototype = new generic_box()

stage_box = function(name, vdict, klist, top, x, y) {
    this.generic_box = generic_box; /* superclass init */
    this.generic_box(name, vdict, klist, top, x, y);
    this.box.draggable = true;
    this.box.addEventListener("dragstart", gui_editor.drag_start)
    this.box.innerHTML = name + '<br> <button onclick="gui_editor.toggle_form(\'fields_' + name + '\')" id="wake_fields_' + name + '"><span class="wakefields"</span></button>' ;
}
stage_box.prototype = new generic_box()

dependency_box = function (name, vdict, klist, top, x, y) {
    this.generic_box = generic_box;
    this.generic_box(name, vdict, klist, top, x, y) /* superclass init */;
    this.stage1 = mwm_utils.trim_blanks(vdict[klist[0]]);
    this.stage2 = mwm_utils.trim_blanks(name.slice(13));
    console.log("trying to do dependency from: " + this.stage1 + " to " + this.stage2)
    this.box.className = 'depbox';
    this.db = document.createElement("DIV");
    this.box.appendChild(this.db);
    this.box.className = 'depbox1';
    this.set_bounds();
}
dependency_box.prototype = new generic_box()

dependency_box.prototype.set_bounds = function () {
   var e1 = document.getElementById("campaign_stage " + this.stage1)   ;
   var e2 = document.getElementById("campaign_stage " + this.stage2)   ;
   if ( e1 == null) {
      console.log("could not find campaign_stage: " + this.stage1)
      return
   }
   if ( e2 == null) {
      console.log("could not find campaign_stage: " + this.stage1)
      return
   }
   var br = this.box.parentNode.getBoundingClientRect();
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

   var midx = (x1+x2)/2

   /* width and height... */
   w = lrx - ulx
   h = lry - uly

   var uphill = (y2 >= y1)

   this.box.style.position='absolute'
   this.box.id = 'dep_' + this.stage1 + '_' + this.stage2
   this.box.style.top = uly.toString()+"px"
   this.box.style.left = ulx.toString()+"px"
   this.box.style.height = h.toString() + "px"
   this.box.style.width = w.toString()+"px"


   this.db.id = 'this.box_box' + this.stage1 + '_' + this.stage2
   if(  uphill ) {
       this.db.className = 'depbox1';
   } else {
       this.db.className = 'depbox1 uphill';
   }
}

