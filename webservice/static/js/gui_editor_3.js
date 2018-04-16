
function dict_size(d) {
   c=0
   for (i in d) {
     c++
   }
   return c
}

function dict_keys(d) {
   res = []
   for (i in d) {
     res.push(i)
   }
   return res
}

gui_editor = function () {
    this.div = document.createElement("DIV");
    this.div.classname = 'gui_editor_frame';
    this.div.style.position='relative';
    document.body.appendChild(this.div);
}

gui_editor.prototype.set_state = function (ini_dump) {
    this.state = JSON.parse(this.ini2json(ini_dump));
    this.draw_state()
}

gui_editor.prototype.trim_blanks = function (s) {
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
      l = this.trim_blanks(lines[i]);

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
          k = this.trim_blanks(k_v.shift());
          v = this.trim_blanks(k_v.join('=')).replace(/"/g,'\\"');
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
   var i, prevstage, prevtype, istr;
   prevtype = ""
   prevstage = ""

   for (k in this.state) {
       if ((k == 'campaign') || ( k == 'global')) {
           ;
       } else if (k.startsWith('campaign_stage')) {
           new stage_box(k, this.state[k], dict_keys(this.state[k]), this.div, x, y)
           /* wimpy layout, assumes tsorted list... */

           if (this.checkdep(k.substr(15), prevstage)) {
               y = y + grid;
           } else {
               x = x + grid;
           }
       } else if (k.startsWith('launch_template')) {
           new generic_box(k, this.state[k], dict_keys(this.state[k]), this.div, x, y)
           if (prevtype != 'launch_template') {
               y = y + grid;
               x = pad;
           }
           prevtype = 'launch_template';
       } else if (k.startsWith('job_type')) {
           new generic_box(k, this.state[k], dict_keys(this.state[k]), this.div, x, y)
           if (prevtype != 'job_type') {
               y = y + grid;
               x = pad;
           }
           prevtype = 'launch_template';
       } else if (k.startsWith('dependencies')) {
          for (i = 1; i <= dict_size(this.state[k])/2; i++) {
              var istr = i.toString()
              new dependency_box(k, this.state[k], ["campaign_stage_"+istr,"file_pattern_"+istr], this.div, x, y);
          }
       } else {
           alert("unknown item " + k);
       }
   }
}

function escape_quotes(s) {
   if (s != undefined) {
       return s.replace(/"/g,'&quot;')
   } else { 
       return s
   }
}

generic_box = function (name, vdict, klist, top, x, y) {
    if (name == undefined) { 
       /* make prototype call work... */
       return;
    }
    this.dict = vdict;
    this.box = document.createElement("DIV");
    this.box.className = "box";
    this.box.id = name
    this.box.style.width = "120px";
    this.box.style.left = x.toString() + "px";
    this.box.style.top = y.toString() + "px";
    this.box.onclick = "toggle_box_selected(name)";
    this.box.innerHTML = name + '<br> <button onclick="toggle_form(\'fields_' + name + '\')" id="wake_fields_' + name + '"><span class="wakefields"</span></button>' ;
    this.popup_parent = document.createElement("DIV");
    this.popup_parent.className = "popup_parent";
    x = x+20;
    y = y+10;
    stage = name.substr(name.indexOf(" ")+1)
    res = [];
    res.push('<form id="fields_' + stage + '" class="popup_form" style="display: none; top: '+ y.toString()+'px; left: ' +x.toString()+'px;">' );
    res.push('<input id="_tag" type="hidden" value="%s">' % stage);
    var val, placeholder;
    for ( k in klist ) {
       if (vdict[k] == null) {
           val="";
           placeholder="defualt";
       } else {
           val=stage_dict[k];
           placeholder="";
       }
       res.push('<label>' + k + '</label> <input id="_inp_' + stage + '_' + k + '" value="' + escape_quotes(val) + '" placeholder="'+placeholder+'"><br>');
    }
    res.push('</form>' );
    this.popup_parent.innerHTML = res.join('\n');
    top.appendChild(this.box);
    top.appendChild(this.popup_parent);
}

stage_box = function(name, vdict, klist, top, x, y) {
    this.generic_box = generic_box
    this.generic_box(name, vdict, klist, top, x, y) /* superclass init */;
    this.box.draggable = true;
    this.box.dragstart="drag_start(event)";
}

dependency_box = function (name, vdict, klist, top, x, y) {
    this.generic_box = generic_box
    this.trim_blanks = gui_editor.prototype.trim_blanks
    this.generic_box(name, vdict, klist, top, x, y) /* superclass init */;
    this.stage1 = this.trim_blanks(vdict[klist[0]]);
    this.stage2 = this.trim_blanks(name.slice(13));
    console.log("trying to do dependency from: " + this.stage1 + " to " + this.stage2)
    this.box.className = 'depbox';
    this.db = document.createElement("DIV");
    this.box.appendChild(this.db);
    this.box.className = 'depbox1';
    this.set_bounds();
}

dependency_box.prototype.set_bounds = function () {
   var e1 = document.getElementById("campaign_stage " + this.stage1)   ;
   var e2 = document.getElementById("campaign_stage " + this.stage2)   ;
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

function getSearchParams(){
 var p={};
 location.search.replace(/[?&]+([^=&]+)=([^&]*)/gi,function(s,k,v){p[k]=v})
 return p;
}

function allowDrop(ev) {
    ev.preventDefault();
}

function drop_it(ev) { 
    ev.preventDefault();
    var id = ev.dataTransfer.getData("text")
    var d = document.getElementById(id)
    if (d != null) {
        d.style.left = ev.x.toString() + "px"
        d.style.top = ev.y.toString() + "px"
    } else {
       alert("could not find: " + id)
    }
    re_draw_dependency_lines(document.edit_dict)
}

function drag_start(ev) {
   ev.dataTransfer.setData("text",ev.target.id)
}

function toggle_form(id) {
    if (document.getElementById(id).style.display == 'block') {
        get_back_values(id)
        document.getElementById(id).style.display = 'none'
    } else {
        document.getElementById(id).style.display = 'block'
    }
}

function toggle_box_selected(id) {
   x =  document.getElementById(id)
   if (x == null) {
      return;
   }
   if (x.className == 'box') {
       x.classList.add('selected')
   } else {
       x.classList.remove('selected')
   }
}
