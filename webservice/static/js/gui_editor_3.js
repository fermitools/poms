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

mwm_utils.getBaseURL = function(){
 var p = location.href.replace(/\/poms.*/,'/poms');
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

mwm_utils.dict_contents = function(d) {
   var res, i;
   res = [];
   for (i in d) {
     res.push(i);
     res.push(d[i]);
   }
   return res.join(":");
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

/* gui editor itself 
 * We setup the <div> that it's all in
 * add drag/drop event handlers, and an initial size
 * and some lists to keep track of boxes...
 * and add ourselves to our class static instance list
 */

function gui_editor(toptag) {
    gui_editor.body = document.getElementById(toptag)
    this.div = document.createElement("DIV");
    this.div.className = 'gui_editor_frame';
    this.div.id = 'gui_editor_' + gui_editor.instance_list.length;
    this.div.gui_box = this;
    this.div.style.position='relative';
    this.div.addEventListener("dragover",gui_editor.dragover_handler);
    this.div.addEventListener("drop", gui_editor.drop_handler);
    this.div.style.width="100%";
    this.div.style.height="200em";
    this.stageboxes = [];
    this.miscboxes = [];
    this.depboxes = [];
    gui_editor.body.appendChild(this.div);
    gui_editor.instance_list.push(this);
}


/* static vars */

gui_editor.body = document.body

/* aforementioned instance list */
gui_editor.instance_list = [];

/* static methods */


/* redraw all dependencies 'cause something moved */
/*  this is where we actually use the instance list */
gui_editor.redraw_all_deps = function() {
   var i;
   for( i in gui_editor.instance_list ) {
       gui_editor.instance_list[i].redraw_deps();
   }
}

/* make form visible/invisible, save on invis */
gui_editor.toggle_form = function(id) {
    var e = document.getElementById(id);
    if (e && e.style.display == 'block') {
        if (e.parentNode && e.parentNode.gui_box) {
            e.parentNode.gui_box.save_values();
        }
        e.style.display = 'none';
    } else if ( e ) {
        e.style.display = 'block';
    }
}

/* make box selected... just use a CSS class */
gui_editor.toggle_box_selected = function(id) {
   var x = document.getElementById(id)
   if (x == null) {
      return;
   }
   if (x.className == 'box') {
       x.classList.add('selected');
   } else {
       x.classList.remove('selected');
   }
}

/* 
 * drag handling code -- 3 handlers...
 *
 * drag_handler:
 * stash the element's id and the x,y coords relative to box in the text data 
 * of the event so we can drop it later
 */
gui_editor.drag_handler = function(ev) {
   if (ev.target == null) {
       return;
   }
   var r = ev.target.getBoundingClientRect();
   var x = ev.x - r.x;
   var y = ev.y - r.y;
   ev.dataTransfer.setData("text",ev.target.id + "@" + x.toString() + "," + y.toString())
}

/*
 * drop_handler -- move the box AND the popup (even though its hidden)
 * there's a little geometry arithmetic to leave it where you dropped it
 */
gui_editor.drop_handler = function(ev) {
    ev.preventDefault();
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
    gui_editor.redraw_all_deps();
}

/*
 * dragover_handler:
 * apparently one needs this so dragging works.. cargo cult
 */
gui_editor.dragover_handler = function(ev) {
    ev.preventDefault();
}

/*
 * callback for delete buttons -- uses the 'gui_box' field we 
 * add to the actual DOM object to find our generic_box object
 * and passes the message on to it -- after a confirm
 */
gui_editor.delete_me = function(id) {
    console.log("trying to delete: " + id)
    var err;
    var e = document.getElementById(id);
    if (e == null){
       return;
    }
    try {
        if (window.confirm("Are you sure you want to delete " + id + "?")) {
            e.gui_box.delete_me();
        }  else {
           console.log("declined..");
        }
    } catch(err) { 
        console.log(err);
    }
    return "ok";
}

gui_editor.newstage = function(id) {
   var e = document.getElementById(id)
   if (e == null){  
       alert("cannot find: " + id)
   }
   e.gui_box.new_stage()
}

gui_editor.makedep = function(id) {
   var e = document.getElementById(id)
   if (e == null){  
       alert("cannot find: " + id)
   }
   e.gui_box.new_dependency()
}
gui_editor.save = function(id) {
   var e = document.getElementById(id)
   if (e == null){  
       alert("cannot find: " + id)
   }
   e.gui_box.save_state()
}

/* pick names workflow clone (below)  */
gui_editor.new_name = function(before, from, to) {
    var after;
    after = before.replace(from, to)
    if (after == before) {
        after = 'clone_of_'+before
    }
    return after
}

/* instance methods */


/* rename stages for a workflow clone */
gui_editor.prototype.clone_rename = function(from, to) {
    var sl, i, j, before, after, jstr, newsl; 
    sl = this.state['campaign']['campaign_stage_list'].split(/  */);
    console.log(["clone_rename: stage list", sl])
    this.state['campaign']['tag'] = gui_editor.new_name(this.state['campaign']['tag'],from, to)
    newsl = []
    for (i in sl) {
         before = sl[i];
         console.log("fixing: " + before)
         after = gui_editor.new_name(before, from ,to);
         this.rename_entity('campaign_stage ' + before ,'campaign_stage ' + after);
         newsl.push(after)
    }
    this.state['campaign']['campaign_stage_list'] = newsl.join(' ')
}


gui_editor.prototype.rename_entity = function(before, after) {
     var e, gb;
     this.state[ after ] = this.state[before];
     delete this.state[before];
     if (before.startsWith('campaign_stage ')) {
         this.fix_dependencies(before.substr(15), after.substr(15));
         if (('dependencies ' + before.substr(15)) in this.state) {
             this.rename_entity('dependencies ' + before.substr(15) ,'dependencies ' + after.substr(15));
         }
     }
     e = document.getElementById(before);
     if ( e ) {
         gb = e.gui_box
         e.innerHTML = e.innerHTML.replace(before,after)
         e.id = after;
         e.gui_box = gb
     }
}

gui_editor.prototype.fix_dependencies = function(before, after) {
     var k ,j, e; 
     for (k in this.state) {
         if (k.startsWith('dependencies ')){
             for( j in this.state[k] ) {
                 if (this.state[k][j] == before) {
                     this.state[k][j] = after; 
                 }
             }
             e = document.getElementById(k);
             if (e && e.gui_box) {
                 if (e.gui_box.stage1 == before) {
                     e.gui_box.stage2 = after;
                 }
                 if (e.gui_box.stage2 == before) {
                     e.gui_box.stage2 = after;
                 }
             }
         }
    }
}

/*
 * set the gui state from an ini-format dump
 */
gui_editor.prototype.set_state_clone = function (ini_dump, from, to) {
    this.state = JSON.parse(this.ini2json(ini_dump));
    this.clone_rename(from, to);
    this.defaultify_state();
    this.draw_state()
}

gui_editor.prototype.set_state = function (ini_dump) {
    this.state = JSON.parse(this.ini2json(ini_dump));
    this.defaultify_state();
    this.draw_state()
}

gui_editor.prototype.defaultify_state = function() {
    var st, k, j, max, maxslot;
    st = {};
    this.mode = {}
    /* count frequency of occurance...*/
    for (k in this.state) {
        if (k.startsWith('campaign_stage')) {
           for (j in this.state[k]) {
               if (!(j in st)) {
                   st[j]={}
               }
               if (! (this.state[k][j] in st[j])) {
                   st[j][this.state[k][j]] = 0;
               }
               st[j][this.state[k][j]]++;
           }
        }
    }
    /* pick the most popular answer for each slot */
    for( j in st ) {
       max = 0;
       maxslot = 0;
       for (k in st[j]) {
           if (st[j][k] > max){
              max = st[j][k];
              maxslot = k;
           }
       }
       this.mode[j] = maxslot;
    }
    /* now null out whatever is the default */
    for (k in this.state) {
       if (k.startsWith('campaign_stage')) {
           for (j in this.state[k]) {
               if (this.state[k][j] == this.mode[j]) {
                   this.state[k][j] = null
               }
           } 
       }
    }
}

gui_editor.prototype.undefaultify_state = function() {
    var k, j;
    for (k in this.state) {
       if (k.startsWith('campaign_stage')) {
           for (j in this.state[k]) {
               if (this.state[k][j] == null ) {
                   this.state[k][j] = this.mode[j]
               }
           } 
       }
    }
}

/*
 * callback from the box object to the gui to clean out
 * ini-section-entry from the state -- if it's empty
 * XXX this should also take the actual box object and delete 
 * them from the stagelist, etc. in the gui state...
 */
gui_editor.prototype.delete_key_if_empty = function (k, box) {
    console.log("delete_key_if_empty:" + k )
    if (k[k.length - 2] == '_') {
       /* for a dependency, we get a name with _1 or _2 etc. on the end */
       k = k.slice(0,-2)
    }
    console.log("delete_key_if_empty -- now:" + k )
    if (k in this.state) {
       console.log("delete_key_if_empty -- saw it...")
       /* 
        * we should have emptied all the fields out before
        * getting here, *unless* we deleted *one* of multiple
        * dependencies from a [dependencies stagename] block
        * so make sure it's empty before actually deleting...
        */
       if (mwm_utils.dict_size(this.state[k]) == 0) {
           delete this.state[k]
       }
    }
    /* clean it form our box lists... */
    var bl, l, i,j ;
    bl = [this.stageboxes, this.miscboxes, this.depboxes]
    for (i in bl) {
        for( j in bl[i] ) {
            if( bl[i][j] == box) {
               console.log("cleaning out" , i , j)
               delete bl[i][j];
            }
        }
    }
}

/*
 * fixup for ini->json conversion
 * we end with: "foo": "bar",
 * and we're about to add a '}', 
 * so dink last comma.
 */
gui_editor.prototype.un_trailing_comma = function(res) {
   if (res.length > 0 && res[res.length-1].slice(-1) == ',') {
     res[res.length-1] = res[res.length-1].slice(0,-1);
   }
}

/* 
 * overall .ini to JSON converter
 * really only does subset of ini file format that 
 * POMS actually generates, doesn't handle colons, 
 * multi-line text blocks, etc.
 * builds a list of strings and joins them, python-style
 */
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
          k_v = l.split(/ *[=:] */);
          console.log(k_v)
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

/*
 * topological sort -- put the list of stages in order left
 *   to right so things we depend on are always at our left
 *   looks like a bubblesort, yes?
 *   I think perhaps we ought to do a transitive closure first
 *   in the general case(?), but our graphs are usually pretty simple
 *   and it seems to work out...
 */
gui_editor.prototype.tsort = function (dlist) {
   var n, i, j, k,  t;
   n = dlist.length;
   for(i = 0; i < n; i++) {
       for(j = 0; j < i; j++ ) {
           if (this.checkdep(dlist[j],dlist[i])) {
               t = dlist[i];
               dlist[i] = dlist[j];
               dlist[j] = t;
           } 
       }
   }
}

/*
 * check if there is a dependecy s2 -> s1
 * only checks for two, probably ought to
 * be a for loop 1..9 or so
 */
gui_editor.prototype.checkdep = function(s1, s2) {
   if (s2 == '' || s1 == '') {
       return 0;
   }
   var k = "dependencies " + s1;

   if (!(k in this.state)) {
       return 0;
   }
   var deps = this.state[k];

   if( ! deps) {
       return 0;
   }
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

/*
 * Top level routine to draw the whole screen
 * -- really only called once
 * -- lots of guestimates about positions and offsets
 * --  particularly stupid about long names/wide boxes
 */
gui_editor.prototype.draw_state = function () {
   var gridx = 220;
   var gridy = 100;
   var labely = 50;
   var pad = 5;
   var x = pad;
   var y = 0;
   var i, prevstage,  istr, b;
   var stagelist, jobtypelist, launchtemplist, k;
   var db, istr, cb, csb;
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

   cb = new label_box("Campaign: " + this.state['campaign']['tag'], this.div, x, y)
   cb.innerHTML += '<button type="button" onclick="gui_editor.save(\'' + this.div.id + '\')">Save</button>';

   y = y + 2 * labely

   csb = new label_box("Campaign Stages:", this.div, x, y)
   csb.innerHTML += '<button type="button" onclick="gui_editor.makedep(\'' + this.div.id + '\')">+ Connect Stages</button>';
   csb.innerHTML += '<button type="button" onclick="gui_editor.newstage(\'' + this.div.id + '\')">+ New Stage</button>';

   var dfb = new misc_box("Default Values", this.mode, mwm_utils.dict_keys(this.mode), this.div, x + 240, y, this)

   y = y + 2 * labely

   /* wimpy layout, assumes tsorted list -- build
    * dependency chains left to right, move other
    * nodes down to next row.
    * probably *ought* to check who the thing
    * actually does depend on and move it one column
    * right of that, but close enough for a first pass
    */

   var first = true
   for( i in stagelist) {
        k = 'campaign_stage ' + stagelist[i];

        if (! first) {
        if (this.checkdep( stagelist[i],prevstage)) {
            x = x + gridx;
        } else {
            y = y + gridy;
        }
        }
        prevstage = stagelist[i]
        first = false
        b = new stage_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y, this)
        this.stageboxes.push(b);
   }

   y = y + 2 * gridy
   x = pad

   new label_box("Job Types:", this.div, x, y)
   y = y + labely

   for (i in jobtypelist) {
        k = 'job_type ' + jobtypelist[i]
        b = new misc_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y, this)
        this.miscboxes.push(b)
        x = x + gridx;
   }

   y = y + gridy
   x = pad 

   new label_box("Login Templates:", this.div, x, y)
   y = y + labely

   for (i in launchtemplist) {
        k = 'launch_template ' + launchtemplist[i];
        b = new misc_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y, this);
        this.miscboxes.push(b)
        x = x + gridx;
    }

    for (k in this.state) {
        if (k.startsWith('dependencies')) {
          for (i = 1; i <= mwm_utils.dict_size(this.state[k])/2; i++) {
              istr = i.toString()
              db = new dependency_box(k+"_"+istr, this.state[k], ["campaign_stage_"+istr,"file_pattern_"+istr], this.div, 0, 0, this);
              this.depboxes.push(db)
          }
       }
   }
   y = y + 2 * gridy;

   this.div.style.height = y.toString()+"px"
}

/*
 * redo the dependecny boxes on this gui_editor
 */
gui_editor.prototype.redraw_deps = function () {
    var k;
    for (k in this.depboxes) {
        this.depboxes[k].set_bounds()
    }
}

/*
 * make a div with a label in it on the overall screen
 * we don't actually track it, as we don't currently try
 * to move it or anything.
 */
function label_box(text, top, x, y) {
    var box = document.createElement("DIV");
    box.style.position='absolute'
    box.style.left = x.toString() + "px";
    box.style.top = y.toString() + "px";
    box.innerHTML = "<h2>" + text + "</h2>"
    top.appendChild(box);
    return box;
}

/*
 * base class for all our boxes on the screen representing
 * sections in the .ini file dump
 * makes a box and a popup form that are both absolutely 
 * placed on the overall gui <div>...
 * -- we subclass this for campaign stage boxes, dependency boxes, and 
 *   other ini file boxes
 */
function generic_box(name, vdict, klist, top, x, y, gui) {
    var i, k, x, y, val , placeholder;
    var stage, res;
    if (name == undefined) { 
       /* make prototype call work... */
       return;
    }
    this.gui = gui
    this.dict = vdict;
    this.klist = klist;
    this.box = document.createElement("DIV");
    this.box.gui_box = this
    this.box.className = "box";
    this.box.id = name
    if (name.length - name.indexOf(' ') > 20) {
        this.box.style.width = "185px";
    } else { 
        this.box.style.width = "120px";
    }
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
    res.push('<h3>' + name );
    if ( name != 'Default Values') {
    res.push('<button title="Delete" class="rightbutton" type="button" onclick="gui_editor.delete_me(\''+name+'\')"><span class="deletebutton"></span></button><p>');
    } 
    res.push('</h3>');
    for ( i in klist ) {
       k = klist[i]
       if (vdict[k] == null) {
           val="";
           placeholder="default";
       } else {
           val=vdict[k];
           placeholder="";
       }
       res.push('<label>' + k + '</label> <input id="' + this.get_input_tag(k) + '" value="' + this.escape_quotes(val) + '" placeholder="'+placeholder+'">');
       if (k.startsWith('param') ) { 
           res.push('<button type="button" onclick="json_field_editor.start(\'' + this.get_input_tag(k) + '\')">Edit</button>')
       }
       res.push('<br>')
    }
    res.push('</form>' );
    this.popup_parent.innerHTML = res.join('\n');
    top.appendChild(this.box);
    top.appendChild(this.popup_parent);
}

/*
 * actual object delete code called by initial click handler
 */
generic_box.prototype.delete_me = function() {
    var i;
    var name = this.box.id
    /* clean up circular reference... */
    this.box.gui_box = null
    this.box.parentNode.removeChild(this.box)
    this.box = null
    this.popup_parent.parentNode.removeChild(this.popup_parent)
    this.popup_parent = null
    /*
     * in a perfect OO implementation we would subclass for these
     * but we can just be polymorphic and check if we have certain
     * object members...
     */
    if ('db' in this) {
        this.db.parentNode.removeChild(this.db)
        this.db = null
    }
    if ('db2' in this) {
        this.db2.parentNode.removeChild(this.db2)
        this.db2 = null
    }
    for( i in this.klist) {
        delete this.dict[this.klist[i]]
    }
    this.gui.delete_key_if_empty(name, this);
    this.gui = null;
    delete this;
}

/*
 * pulled out to share input tag name figuring between
 * generate and save code...
 */
generic_box.prototype.get_input_tag = function(k) {

    return "_inp" + this.box.id + '_' + k
}

/*
 * save values from a popup form back into the state.
 */
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

/*
 * convert quotes to &quot; for <input value="xxx'> values
 * should be a  astatic method...
 */
generic_box.prototype.escape_quotes = function(s) {
   if (s != undefined) {
       return s.replace(/"/g,'&quot;')
   } else { 
       return s
   }
}

/*
 * non-draggable, non-dependency box class
 * for job_types, etc.  subclass of generic_box
 */
function misc_box(name, vdict, klist, top, x, y, gui) {
    this.generic_box = generic_box; /* superclass init */
    this.generic_box(name, vdict, klist, top, x, y, gui);
    this.box.innerHTML = name + '<br> <button type="button" onclick="gui_editor.toggle_form(\'fields_' + name + '\')" id="wake_fields_' + name + '"><span class="wakefields"></span></button>' ;
}
misc_box.prototype = new generic_box()

/*
 * box to represent campaign stages -- draggable, etc.
 */
function stage_box(name, vdict, klist, top, x, y, gui) {
    this.generic_box = generic_box; /* superclass init */
    this.generic_box(name, vdict, klist, top, x, y, gui);
    this.box.draggable = true;
    this.box.addEventListener("dragstart", gui_editor.drag_handler)
    this.box.innerHTML = name + '<br> <button type="button" onclick="gui_editor.toggle_form(\'fields_' + name + '\')" id="wake_fields_' + name + '"><span class="wakefields"></span></button>' ;
}
stage_box.prototype = new generic_box()

/*
 * box to represent dependencies -- note that 
 * you generally only see two borders of these boxes, and the button
 */
function dependency_box(name, vdict, klist, top, x, y, gui) {
    this.generic_box = generic_box;
    this.generic_box(name, vdict, klist, top, x, y, gui) /* superclass init */;
    this.stage1 = mwm_utils.trim_blanks(this.dict[this.klist[0]]);
    this.stage2 = mwm_utils.trim_blanks(name.slice(13, -2)); /* has a _1 or _2 on the end AND a 'campaign_stage ' on the front */
    /* this.box.id = 'dep_' + this.stage1 + '_' + this.stage2 */
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

/*
 * routine to (re)set the bounds of a dependency box 
 * so its corners are centered on the two states that
 * it involves.
 * note that this also moves the popup form for the
 * dependency to be next to it (even while hidden)
 */
dependency_box.prototype.set_bounds = function () {
   console.log("set_bounds('"+ this.stage1+ "' , '" + this.stage2 + "')")
   var e1 = document.getElementById("campaign_stage " + this.stage1);
   var e2 = document.getElementById("campaign_stage " + this.stage2);
   if ( e1 == null) {
      console.log("could not find campaign_stage: '" + this.stage1+ "'")
      return
   }
   if ( e2 == null) {
      console.log("could not find campaign_stage: '" + this.stage2 + "'")
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

   var circular = (this.stage1 == this.stage2)

   if (circular) {
       w=30
       h=70
       lry += 70
       lrx += 30
   }

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

   if(  circular  ) {
       this.db.className = 'depbox1 circular';
   } else if (uphill)  {
       this.db.className = 'depbox1 uphill';
   } else {
       this.db.className = 'depbox1 downhill';
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

gui_editor.prototype.new_stage = function () {
    var k = window.prompt("New stage name:")
    var x, y, b;
    this.state['campaign']['campaign_stage_list'] += " " + k
    k = 'campaign_stage ' + k
    this.state[k] = {
	'dataset': null,
	'software_version': null,
	'vo_role': null,
	'cs_split_type': null,
	'job_type': null,
	'launch_template': null,
	'param_overrides': null,
	'completion_type': null,
	'completion_pct': null
    }
    x = 500;
    y = 150;
    b = new stage_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y, this)
    this.stageboxes.push(b);
}

gui_editor.prototype.new_dependency = function() {
    var elist1, elist, k1, istr, db, s1, s2, i;
    elist1 = this.div.getElementsByClassName('selected')
    elist = []
    for (i = 0; i < elist1.length; i++) {
        elist.push(elist1[i]);
    }
  
    console.log(['selected', elist])
    
    if (elist.length < 2 || elist.length > 2) {
        window.alert("Need exactly two Campagn Stages selected")
        return
    }
    /* we think the left-to-right position on the screen indicates
     * which way the dependency should go, so if they're the other
     * way around, swap them...
     */
   if(elist[0].getBoundingClientRect().x > elist[1].getBoundingClientRect().x) {
       var t = elist[0];
       elist[0] = elist[1];
       elist[1] = t;
   }
   s1 = elist[0].id.replace("campaign_stage ","")
   s2 = elist[1].id.replace("campaign_stage ","")
   k1 = 'dependencies ' + s2
   if (k1 in this.state) {
       istr =((mwm_utils.dict_size(this.state[k1]) / 2) + 1).toString()
   } else {
       this.state[k1] = {};
       istr = '1';
   }
   console.log('elist:')
   console.log(elist)

   this.state[k1]['campaign_stage_' + istr] = s1
   this.state[k1]['file_pattern_' + istr] = '%'
   db = new dependency_box(k1+"_"+istr, this.state[k1], ["campaign_stage_"+istr,"file_pattern_"+istr], this.div, 0, 0, this);
   this.depboxes.push(db)
}

gui_editor.prototype.save_state = function() {
   var wu = new wf_uploader( function(){ this.unbuzy()})
   this.undefaultify_state()
   wu.upload(this.state)
   this.defaultify_state()
}


/*
 * ===================================================================
 * uploader -- translated from the upload_wf code in poms_client...
 */
function wf_uploader(on_complete) {
    var i, s, l, jt;
    this.cfg = null;
}
 
wf_uploader.prototype.upload = function(state) {
    this.cfg = state;
    var headers =this.get_headers()
    this.username = headers['X-Shib-Userid']
    this.experiment = state['campaign']['experiment']
    var role = state['campaign']['poms_role'];
    this.update_session_role(role);
    var cfg_stages = this.cfg['campaign']['campaign_stage_list'].split(' ');
    var cfg_jobtypes = {};
    var cfg_launches = {};
    var i, l, jt, s;
    this.cname_id_map = this.get_campaign_list();
    for( i in cfg_stages) {
        s = cfg_stages[i];
        cfg_jobtypes[this.cfg['campaign_stage ' +s]['job_type']] = 1
        cfg_launches[this.cfg['campaign_stage ' +s]['launch_template']] = 1
    }
    for( l in cfg_launches) {
        this.upload_launch_template(l)
    }
    for(jt in cfg_jobtypes) {
        this.upload_jobtype(jt)
    }
    for( i in cfg_stages) {
        s = cfg_stages[i];
        this.upload_stage(s)
    }

    this.tag_em(this.cfg['campaign']['tag'],cfg_stages)
}

wf_uploader.prototype.tag_em =  function(tag, cfg_stages) {
    var cids = cfg_stages.map(x => this.cname_id_map[x].toString());
    /* have to re-fetch the list, if we added any campaigns... */
    this.cname_id_map = this.get_campaign_list();
    var args = { 'tag_name': tag, 'campaign_id': cids.join(','), 'experiment': this.cfg['campaign']['experiment'] };
    this.make_poms_call('link_tags',args);
}

wf_uploader.prototype.upload_jobtype =  function(jt) {
    var field_map, k, d, args;
    field_map = {
            'launch_script':'ae_launch_script',
            'parameters':'ae_definition_parameters',
            'output_file_patterns':'ae_output_file_patterns',
        };
    d = this.cfg['job_type ' + jt]
    args = {
        'pcl_call': '1',
        'pc_username': this.username,
        'action': 'add',
        'ae_definition_name': jt,
        'experiment': this.cfg['campaign']['experiment'],
    }
    for(k in d) {
         if (k in field_map) {
            args[field_map[k]] = d[k]
         } else {
            args[k] = d[k]
         }
    }
     /* there are separate add/update calls; just do both, if it
      * exists already, the first will fail.. 
      */
    this.make_poms_call('campaign_definition_edit', args)
    args['action'] = 'edit'
    this.make_poms_call('campaign_definition_edit', args)
}

wf_uploader.prototype.upload_launch_template =  function(l) {
    var field_map, d, args, k ;
    field_map = {
            'host': 'ae_launch_host',
            'account': 'ae_launch_account',
            'setup': 'ae_launch_setup',
    };
    d = this.cfg['launch_template ' + l]
    console.log(['d',d])
    args  = {
             'action': 'add', 
             'pcl_call': '1',
             'pc_username': this.username,
             'ae_launch_name': l, 
             'experiment': this.cfg['campaign']['experiment']
        }
    for(k in d) {
         if (k in field_map) {
            args[field_map[k]] = d[k]
         } else {
            args[k] = d[k]
         }
     }
     this.make_poms_call('launch_template_edit', args)
     args['action'] = 'edit'
     this.make_poms_call('launch_template_edit', args)
}

wf_uploader.prototype.upload_stage =  function(st) {
    var i, dst, field_map, deps, d, args, k, pat;
    field_map = {
            'dataset': 'ae_dataset',
            'software_version': 'ae_software_version',
            'vo_role': 'ae_vo_role',
            'cs_split_type': 'ae_split_type',
            'job_type': 'ae_campaign_definition',
            'launch_template': 'ae_launch_name',
            'param_overrides': 'ae_param_overrides',
            'completion_type': 'ae_completion_type',
            'completion_pct': 'ae_completion_pct',
        };
    deps = {"file_patterns":[], "campaigns":[]}
    for (i = 0; i< 10; i++ ) {
        if ((('dependencies ' + st) in this.cfg) && ('campaign_stage_'+i.toString()) in this.cfg['dependencies ' + st]) {
            dst = this.cfg['dependencies ' + st]['campaign_stage_'+i.toString()]
            pat = this.cfg['dependencies ' + st]['file_pattern_'+i.toString()]
            deps["campaigns"].push(dst)
            deps["file_patterns"].push(pat)
        }
    }  
    d = this.cfg['campaign_stage '+st]
    args = {
            'pcl_call': '1',
            'pc_username': this.username,
            'action': (st in this.cname_id_map)?'edit':'add',
            'ae_campaign_name': st,  
            'experiment': this.cfg['campaign']['experiment'], 
            'ae_active': true, 
            'ae_depends': JSON.stringify(deps),
        }
    for(k in d) {
         if (k in field_map) {
            args[field_map[k]] = d[k]
         } else {
            args[k] = d[k]
         }
     }
     this.make_poms_call('campaign_edit', args)
}

wf_uploader.prototype.get_campaign_list = function() {
     var x, res, i, pair;
     x =  this.make_poms_call('campaign_list_json', {})
     res = {}
     for( i in x) {
         pair = x[i];
         if (pair.experiment == this.experiment) {
             res[pair.name] = pair.campaign_id;
         }
     }
     return res;
}

wf_uploader.prototype.update_session_role = function(role) {
     return this.make_poms_call('update_session_role', {'session_role': role})
}

wf_uploader.prototype.get_headers= function() {
    var s = this.make_poms_call('headers', {});
    s = s.replace(/\'/g,'"')
    return JSON.parse(s);
}

wf_uploader.prototype.make_poms_call = function(name, args) {
     var k, res;
     var base = mwm_utils.getBaseURL()
     console.log(['make_poms_call',name,args])
     for (k in args) {
          if (args[k] == null || args[k] == undefined) {
              delete args[k];
          }
     }
     jQuery.ajax({
        url: base + '/' + name,
        data: args,
        method: args ? 'POST':'GET',
        success: function(result) {
            res = result;
        }, 
        error: function(result) {
            var p, resp;
            p = result.responseText.indexOf('>Traceback');
            if (p > 0) {
                resp = result.responseText.slice(p+6,);
                p = resp.indexOf('</label>')
                if(p < 0) {
                    p = resp.indexOf('</pre>')
                }
                resp = resp.slice(0,p)
                resp.replace(/<br\/>/g,'\n')
            } else {
                resp = result.responseText;
            }
            console.log(resp);
        },
        async: false
     });
     return res;
}

