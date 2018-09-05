"use strict";

/* utility function bundle */

function mwm_utils() {
   /* constructor does nothing... */
   return;
}

/* static functions for use elsewhere */

/* get url search parameters (i.e after "?" on url) */
mwm_utils.getSearchParams = function () {
    var p = {};
    location.search.replace(/[?&]+([^=&]+)=([^&]*)/gi, function (s, k, v) { p[k] = v });
    return p;
}

mwm_utils.getBaseURL = function () {
    var p = location.href.replace(/(.*:\/\/[^\/]*\/[^\/]*\/).*/, '$1')
    return p;
}

/* return count of pairs in dictionary */
mwm_utils.dict_size = function (d) {
    var c, i;
    c = 0;
    for (i in d) {
        c++;
    }
    return c;
}

mwm_utils.dict_contents = function (d) {
    var res, i;
    res = [];
    for (i in d) {
        res.push(i);
        res.push(d[i]);
    }
    return res.join(":");
}

/* return list of keys in dictionary */
mwm_utils.dict_keys = function (d) {
    var res, i;
    res = [];
    for (i in d) {
        res.push(i);
    }
    return res;
}

/* return string without leading/traling blanks */
mwm_utils.trim_blanks = function (s) {
/*     var i, j;
    i = 0;
    if (s === undefined) {
        return '';
    }
    j = s.length;
    while (s[i] == ' ') {
        i++;
    }
    while (s[j - 1] == ' ') {
        j--;
    }
    return s.slice(i, j);
 */
    return (typeof s === 'undefined' ? '' : s.trim());
}

mwm_utils.formFields = function (el) {
    return $(el).find('input,select').toArray().map(x => [x.name, x.value]).reduce((a, v) => ({...a, [v[0]]: v[1]}), {});
}

mwm_utils.hashCode = function (str) {
    return str.split('').reduce((prevHash, currVal) => (((prevHash << 5) - prevHash) + currVal.charCodeAt(0))|0, 0).toString(16);
};

/* gui editor itself
 * We setup the <div> that it's all in
 * add drag/drop event handlers, and an initial size
 * and some lists to keep track of boxes...
 * and add ourselves to our class static instance list
 */

function gui_editor(toptag) {
    gui_editor.body = document.getElementById(toptag);

    this.div = document.createElement("DIV");
    this.div.className = 'gui_editor_frame';
    this.div.id = 'gui_editor_' + gui_editor.instance_list.length;
    this.div.gui_box = this;
    this.div.style.position = 'relative';
    this.div.addEventListener("dragover", gui_editor.dragover_handler);
    this.div.addEventListener("drop", gui_editor.drop_handler);
    this.div.style.width = "100%";
    this.div.style.height = "200em";
    this.stageboxes = [];
    this.miscboxes = [];
    this.depboxes = [];
    gui_editor.body.appendChild(this.div);
    gui_editor.instance_list.push(this);

    //
    this.jobtypes = [];
}


/* static vars */

    gui_editor.body = document.body;
    gui_editor.network = null;

/* aforementioned instance list */
    gui_editor.instance_list = [];

/* static methods */

gui_editor.modified = function () {
    /* we really ought to have a savebusy per instance, and make it a class but... */
    var sb = document.getElementById("savebusy");
    sb.innerHTML = "Modified";

    window.onbeforeunload = function (e) {
        var msg = "Workflow has been changed, but not saved.  Are you sure you want to leave?";
        e.returnValue = msg;
        return msg;
    }
}

gui_editor.unmodified = function () {
    /* we really ought to have a savebusy per instance, and make it a class but... */

    window.onbeforeunload = undefined;
}

/* redraw all dependencies 'cause something moved */
/*  this is where we actually use the instance list */
gui_editor.redraw_all_deps = function () {
    var i;
    for (i in gui_editor.instance_list) {
        gui_editor.instance_list[i].redraw_deps();
    }
}

/* make form visible/invisible, save on invis */
gui_editor.toggle_form = function(id) {
    gui_editor.modified()
    var e = document.getElementById(id);
    if (e && e.style.display == 'block') {
        if (e.parentNode && e.parentNode.gui_box) {
            e.parentNode.gui_box.save_values();
            if (id.includes('campaign')) {
                const nm = id.split(' ')[1];        // component name
                const nname = id.includes('_stage') ? nm : `campaign ${nm}`;                    // build node name
                //VP~ this.nodes.update([{id: nname, label: e.name.value}]);
                gui_editor.network.body.data.nodes.update({id: nname, label: e.name.value});    // update label
            }
        }
        e.style.display = 'none';
    } else if ( e ) {
        e.style.display = 'block';
    }
}

/* make box selected... just use a CSS class */
gui_editor.toggle_box_selected = function (id) {
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
gui_editor.drag_handler = function (ev) {
    ev = ev || window.event;
    if (ev.target == null) {
        return;
    }
    var r = ev.target.getBoundingClientRect();
    var x = ev.clientX - r.left;
    var y = ev.clientY - r.top;
    ev.dataTransfer.setData("text", ev.target.id + "@" + x.toString() + "," + y.toString())
}

/*
 * drop_handler -- move the box AND the popup (even though its hidden)
 * there's a little geometry arithmetic to leave it where you dropped it
 */
gui_editor.drop_handler = function (ev) {
    ev = ev || window.event;
    ev.preventDefault();
    var idatxy = ev.dataTransfer.getData("text");
    var idatxyl = idatxy.split(/[@,]/g);
    var id = idatxyl[0];
    console.log("idatxy is " + idatxy);
    console.log("clickx: " + idatxyl[1]);
    console.log("clicky: " + idatxyl[2]);
    var clickx = parseInt(idatxyl[1]);
    var clicky = parseInt(idatxyl[2]);
    var d = document.getElementById(id);
    var f = document.getElementById('fields_' + id);
    var r = d.parentNode.getBoundingClientRect();
    if (d != null) {
        console.log("found id");
        d.style.left = (ev.clientX - clickx - r.left).toString() + "px";
        d.style.top = (ev.clientY - clicky - r.top).toString() + "px";
    }
    if (f != null) {
        console.log("found form");
        f.style.left = (ev.clientX - clickx - r.left + 50).toString() + "px";
        f.style.top = (ev.clientY - clicky - r.top + 50).toString() + "px";
    }
    gui_editor.redraw_all_deps();
}

/*
 * dragover_handler:
 * apparently one needs this so dragging works.. cargo cult
 */
gui_editor.dragover_handler = function(ev) {
    ev = ev || window.event;
    ev.preventDefault();
}

/*
 * callback for delete buttons -- uses the 'gui_box' field we
 * add to the actual DOM object to find our generic_box object
 * and passes the message on to it -- after a confirm
 */
gui_editor.delete_me = function(id) {
    console.log("trying to delete: " + id);
    var err;
    var e = document.getElementById(id);
    if (e == null) {
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

gui_editor.newstage = function (id) {
    gui_editor.modified();
    var e = document.getElementById(id);
    if (e == null) {
        alert("cannot find: " + id);
    }
    e.gui_box.new_stage();
}

gui_editor.makedep = function (id) {
    gui_editor.modified();
    var e = document.getElementById(id);
    if (e == null) {
        alert("cannot find: " + id);
    }
    e.gui_box.new_dependency();
}

gui_editor.save = function (id) {
    var e = document.getElementById(id);
    if (e == null) {
        alert("cannot find: " + id);
    }
    e.gui_box.save_state();
    //VP~ gui_editor.exportNetwork();
}

/* pick names workflow clone (below)  */
gui_editor.new_name = function (before, from, to) {
    var after;
    after = before.replace(from, to);
    if (after == before) {
        after = 'clone_of_' + before;
    }
    return after;
}


gui_editor.exportNetwork = function () {
    // var nodes = gui_editor.network.body.nodeIndices.map(x => ({id: x}));

    const node2JSON = (e) => {
        //VP~ const ename = e[0].startsWith('Default') ? `fields_${e[0]}` : `fields_campaign_stage ${e[0]}`;
        //VP~ const ename = e[0].startsWith('campaign ') ? `fields_${e[0]}` : `fields_campaign_stage ${e[0]}`;
        const ename = e[0].match(/campaign |job_type |login_setup /) ? `fields_${e[0]}` : `fields_campaign_stage ${e[0]}`;
        const el = document.getElementById(ename);
        const ff = mwm_utils.formFields(el);
        const hval = mwm_utils.hashCode(JSON.stringify(ff));
        const oval = $(el).attr('data-hash');
        const response = {id: e[0],
                          label: network.body.nodes[e[0]].options.label,
                          position: e[1],
                          clean: hval === oval ? true : false,
                          form: ff
                };
        //VP~ return e[0].startsWith("campaign ") ? {campaign: this.state.campaign, ...response} : response;    // Not yet, 'this' is not available here
        return response;
    };

    let network = gui_editor.network;
    var nodes = Object.entries(gui_editor.network.getPositions()).map(node2JSON);
    network = gui_editor.aux_network;
    var aux = Object.entries(gui_editor.aux_network.getPositions()).map(node2JSON);

    // nodes.forEach(addConnections);

    var edges = Object.entries(gui_editor.network.body.edges)
                        .map(e => {
                                    const ename = `fields_${e[0]}`;
                                    const el = document.getElementById(ename);
                                    //VP~ const ff = $(el).find('input').toArray().map(x => [x.name, x.value]).reduce((a, v) => ({...a, [v[0]]: v[1]}), {});
                                    const ff = mwm_utils.formFields(el);
                                    const hval = mwm_utils.hashCode(JSON.stringify(ff));
                                    const oval = $(el).attr('data-hash');
                                    return {
                                        id: e[0],
                                        //fromId:e[1].fromId,
                                        //toId:e[1].toId,
                                        fromId:e[1].from.options.label,
                                        toId:e[1].to.options.label,
                                        clean: hval === oval ? true : false,
                                        form: ff
                                    }
                            }
                        );
    // pretty print node data
    var exportValue = JSON.stringify({stages: nodes, dependencies: edges, misc: aux}, undefined, 2);
    /*
    function addConnections(elem, index) {
        // elem.connections = network.getConnectedNodes(elem.id);
        elem.connections = gui_editor.network.getConnectedEdges(elem.id).filter(x => gui_editor.network.body.edges[x].toId != elem.id).map(x => gui_editor.network.body.edges[x].toId);
    }
    */
    console.log(exportValue);
    //VP~ return exportValue;
    //VP~ new wf_uploader().make_poms_call('echo', {form: exportValue});     // Send to the server
    new wf_uploader().make_poms_call('save_campaign', {form: exportValue});     // Send to the server
}


/* instance methods */


/* rename stages for a workflow clone */
gui_editor.prototype.clone_rename = function (from, to, experiment, role) {
    console.log(["clone_rename:", from, to, experiment, role]);
    var stages, before, after;
    stages = this.state['campaign']['campaign_stage_list'].split(/  */);
    console.log(["clone_rename: stage list", stages]);
    this.state['campaign']['name'] = gui_editor.new_name(this.state['campaign']['name'], from, to);

    let new_stages = [];
    if (experiment != undefined) {
        this.state['campaign']['experiment'] = experiment;
    }
    if (role != undefined) {
        this.state['campaign']['poms_role'] = role;
    }
    console.log(["clone_rename: campaign fields", this.state['campaign']]);
    for (let i in stages) {
        before = stages[i];
        console.log("fixing: " + before);
        after = gui_editor.new_name(before, from, to);
        this.rename_entity('campaign_stage ' + before, 'campaign_stage ' + after);
        new_stages.push(after);
    }
    this.state['campaign']['campaign_stage_list'] = new_stages.join(' ');
}


gui_editor.prototype.rename_entity = function (before, after) {
    var e, gb;
    this.state[after] = this.state[before];
    delete this.state[before];
    if (before.indexOf('campaign_stage ') == 0) {
        this.fix_dependencies(before.substr(15), after.substr(15));
        if (('dependencies ' + before.substr(15)) in this.state) {
            this.rename_entity('dependencies ' + before.substr(15), 'dependencies ' + after.substr(15));
        }
    }
    e = document.getElementById(before);
    if (e) {
        gb = e.gui_box;
        e.innerHTML = e.innerHTML.replace(before, after);
        e.id = after;
        e.gui_box = gb;
    }
}

gui_editor.prototype.fix_dependencies = function (before, after) {
    var k, j, e;
    for (k in this.state) {
        if (k.indexOf('dependencies ') == 0) {
            for (j in this.state[k]) {
                if (this.state[k][j] == before) {
                    this.state[k][j] = after;
                }
            }
            e = document.getElementById(k);
            if (e && e.gui_box) {
                if (e.gui_box.stage1 == before) {
                    //e.gui_box.stage2 = after;
                    e.gui_box.stage1 = after;
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
gui_editor.prototype.set_state_clone = function (ini_dump, from, to, experiment, role) {
    const r = new wf_uploader().make_poms_call('jobtype_list', null).then(
        (data) => {
            for (const val of data) {
                console.log('jobtype=', val.name);
                this.jobtypes.push(val.name);
            }
        }).then(
        _ => {
            this.state = JSON.parse(this.ini2json(ini_dump));
            this.clone_rename(from, to, experiment, role);
            this.defaultify_state();
            this.draw_state();
        }
    );
}

gui_editor.prototype.set_state = function (ini_dump) {
    const r = new wf_uploader().make_poms_call('jobtype_list', null).then(
        (data) => {
            for (const val of data) {
                console.log('jobtype=', val.name);
                this.jobtypes.push(val.name);
            }
        }).then(
        _ => {
            this.state = JSON.parse(this.ini2json(ini_dump));
            console.log("State:\n", this.state);    // DEBUG
            this.defaultify_state();
            this.draw_state();
        }
    )
}

gui_editor.prototype.defaultify_state = function() {
    var st, k, j, max, maxslot;
    st = {};
    this.mode = {}
    /* count frequency of occurance...*/
    for (k in this.state) {
        if (k.indexOf('campaign_stage') == 0) {
            for (j in this.state[k]) {
                if (!(j in st)) {
                    st[j] = {}
                }
                if (!(this.state[k][j] in st[j])) {
                    st[j][this.state[k][j]] = 0;
                }
                st[j][this.state[k][j]]++;
            }
        }
    }
    /* pick the most popular answer for each slot */
    for (j in st) {
        max = -1;
        maxslot = -1;
        for (k in st[j]) {
            if (st[j][k] > max) {
                max = st[j][k];
                maxslot = k;
            }
        }
        this.mode[j] = maxslot;
    }
    this.mode.name = this.state.campaign.name;  // 'Name' is special case - store the campaign name!
    /* now null out whatever is the default */
    for (k in this.state) {
        if (k.indexOf('campaign_stage') == 0) {
            for (j in this.state[k]) {
                if (this.state[k][j] == this.mode[j]) {
                    this.state[k][j] = null;
                }
            }
        }
    }
}

gui_editor.prototype.undefaultify_state = function () {
    var k, j;
    for (k in this.state) {
        if (k.indexOf('campaign_stage') == 0) {
            for (j in this.state[k]) {
                if (this.state[k][j] == null) {
                    this.state[k][j] = this.mode[j];
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
    console.log("delete_key_if_empty:" + k);
    if (k[k.length - 2] == '_') {
        /* for a dependency, we get a name with _1 or _2 etc. on the end */
        k = k.slice(0, -2);
    }
    console.log("delete_key_if_empty -- now:" + k);
    if (k in this.state) {
        console.log("delete_key_if_empty -- saw it...");
        /*
         * we should have emptied all the fields out before
         * getting here, *unless* we deleted *one* of multiple
         * dependencies from a [dependencies stagename] block
         * so make sure it's empty before actually deleting...
         */
        if (mwm_utils.dict_size(this.state[k]) == 0) {
            delete this.state[k];
        }
    }
    /* clean it form our box lists... */
    var bl, l, i, j;
    bl = [this.stageboxes, this.miscboxes, this.depboxes];
    for (i in bl) {
        for (j in bl[i]) {
            if (bl[i][j] == box) {
                console.log("cleaning out", i, j);
                delete bl[i][j];
            }
        }
    }
    /* if it is a campaign stage, remove it from the campaign stage list */
    if (k.indexOf('campaign_stage ') == 0) {
        let sl, stage;
        stage = k.slice(15);
        sl = this.state['campaign']['campaign_stage_list'].split(/  */);
        sl = sl.filter(function (x) { return x != stage && x != ''; })
        this.state['campaign']['campaign_stage_list'] = sl.join(' ')
    }
}

/*
 * fixup for ini->json conversion
 * we end with: "foo": "bar",
 * and we're about to add a '}',
 * so dink last comma.
 */
gui_editor.prototype.un_trailing_comma = function (res) {
    if (res.length > 0 && res[res.length - 1].slice(-1) == ',') {
        res[res.length - 1] = res[res.length - 1].slice(0, -1);
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
      if (l[0] == '#') {                                    // Comment line
          continue;
      } else if (l[0] == '[' &&  l[l.length-1]  == ']') {   // Section
          const s = l.slice(1, -1);
          const sn = s.split(' ')[1];
          this.un_trailing_comma(res);
          res.push('},');
          res.push('"' + s + '": {');
          if (sn) {
            res.push(`"name": "${sn}",`);                   // Add section name as a value
          }
      } else {                                              // Section body
          l = mwm_utils.trim_blanks(l)
          l = l.replace(/%%/g,'%');
          k_v = l.match(/([^ =:]*) *[=:] *(.*)/);
          console.log(k_v)
          k_v.shift();
          k = k_v.shift();
          v = k_v.join('=').replace(/"/g,'\\"');
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
   console.log({"result": res.join("\n")})
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
    var n, i, j, k, t;
    n = dlist.length;
    for (i = 1; i < n; i++) {
        for (j = 0; j < i; j++) {
            if (this.checkdep(dlist[j], dlist[i])) {
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
gui_editor.prototype.checkdep = function (s1, s2) {
    console.log("Testing...", s1, '->', s2);
    if (s2 == '' || s1 == '') {
        return 0;
    }
    var depname = "dependencies " + s1;

    if (!(depname in this.state)) {
        return 0;
    }
    var deps = this.state[depname];

    if (!deps) {
        //console.log("\tDeps empty...");
        return 0;
    }
    if (!('campaign_stage_1' in deps)) {
        //console.log("\tNo parent...");
        return 0;
    }
    if (deps['campaign_stage_1'] == s2) {
        return 1;
    }
    else  {
        return this.checkdep(s1, deps['campaign_stage_1']);
    }
    //
    // Check more parents
    if (!('campaign_stage_2' in deps)) {
        return 0;
    }
    if (deps['campaign_stage_2'] == s2) {
        return 1;
    }
    return 0;
}

gui_editor.prototype.getdepth = function (s1, cnt) {
    if (s1 == '') {
        return cnt;
    }
    var k = "dependencies " + s1;

    if (!(k in this.state)) {
        return cnt;
    }
    var deps = this.state[k];

    if (!deps) {
        return cnt;
    }
    if (!('campaign_stage_1' in deps)) {
        return cnt;
    }
    if (!(deps['campaign_stage_1'])) {
        return cnt;
    }
    return this.getdepth(deps['campaign_stage_1'], cnt+1);
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
    var i, prevstage, istr, b;
    var stagelist, launchtemplist, k;
    var db, istr, cb, csb;
    prevstage = "";

    stagelist = [];
    this.jobtypelist = [];
    launchtemplist = [];

    for (k in this.state) {
        const n = k.split(' ')[1];
        if (k.indexOf('campaign_stage') == 0) {
            //stagelist.push(k.slice(15));
            stagelist.push(n);
        } else if (k.indexOf('job_type') == 0) {
            //this.jobtypelist.push(k.slice(9));
            this.jobtypelist.push(n);
        } else if (k.indexOf('login_setup') == 0) {
            //launchtemplist.push(k.slice(12));
            launchtemplist.push(n);
        }
    }

    //this.tsort(stagelist);
    stagelist.sort((a, b) => 1 - this.checkdep(a, b)).reverse();

    cb = new label_box("Campaign: " + this.state['campaign']['name'], this.div, x, y);
    cb.innerHTML += `<button type="button" onclick="gui_editor.save('${this.div.id}')">Save</button> <span id="savebusy"></span>`;

    y = y + 2 * labely;

    csb = new label_box("Campaign Stages:", this.div, x, y);
    csb.innerHTML += `<button type="button" onclick="gui_editor.makedep('${this.div.id}')">+ Connect Stages</button>`;
    csb.innerHTML += `<button type="button" onclick="gui_editor.newstage('${this.div.id}')">+ New Stage</button>`;

    //VP~ var dfb = new misc_box("Default Values", this.mode, mwm_utils.dict_keys(this.mode), this.div, x + 240, y, this);
    var dfb = new misc_box(`campaign ${this.state.campaign.name}`, this.mode, mwm_utils.dict_keys(this.mode), this.div, x + 240, y, this);

    y = y + 2 * labely;

   /* wimpy layout, assumes tsorted list -- build
    * dependency chains left to right, move other
    * nodes down to next row.
    * probably *ought* to check who the thing
    * actually does depend on and move it one column
    * right of that, but close enough for a first pass
    */

    var first = true;
    for (i in stagelist) {
        k = 'campaign_stage ' + stagelist[i];

        if (!first) {
            //if (this.checkdep(stagelist[i], prevstage)) {
                //x = x + gridx * this.getdepth(stagelist[i], 0);
            if (this.getdepth(stagelist[i], 0) != this.getdepth(prevstage, 0)) {
                x = gridx * this.getdepth(stagelist[i], 0);
            }
            else {
                //x = gridx * this.getdepth(stagelist[i], 0);
                y = y + gridy;
            }
        }
        prevstage = stagelist[i];
        first = false;
        b = new stage_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y, this);
        this.stageboxes.push(b);
    }

    y = y + 2 * gridy;
    x = pad;

    new label_box("Job Types:", this.div, x, y);
    y = y + labely;

    for (i in this.jobtypelist) {
        k = 'job_type ' + this.jobtypelist[i];
        b = new misc_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y, this);
        this.miscboxes.push(b);
        x = x + gridx;
    }

    y = y + gridy;
    x = pad;

    new label_box("Login/Setup:", this.div, x, y);
    y = y + labely;

    for (i in launchtemplist) {
        k = 'login_setup ' + launchtemplist[i];
        b = new misc_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y, this);
        this.miscboxes.push(b);
        x = x + gridx;
    }

    for (k in this.state) {
        if (k.indexOf('dependencies') == 0) {
            for (i = 1; i <= mwm_utils.dict_size(this.state[k]) / 2; i++) {
                istr = i.toString();
                db = new dependency_box(k + "_" + istr, this.state[k], ["campaign_stage_" + istr, "file_pattern_" + istr], this.div, 0, 0, this);
                this.depboxes.push(db);
            }
        }
    }
    y = y + 2 * gridy;

    this.div.style.height = y.toString() + "px";

    /*
     * Vis.js stuff
     */
    const depFrom = (label) => {
        const dep = this.state[`dependencies ${label}`];
        const froms = Object.keys(dep).filter(x => x.startsWith("campaign_stage_")).map(x => dep[x]);
        return froms;
    };

    let node_labels = Object.keys(this.state).filter(x => x.startsWith("campaign_stage ")).map(x => x.split(' ')[1]);
    let node_list = node_labels.map(x => ({id:x, label:x, group: this.getdepth(x, 1)}));
    //VP~ this.nodes = new vis.DataSet([{id: 'Default Values', label: this.state.campaign.name,
    this.nodes = new vis.DataSet([{id: `campaign ${this.state.campaign.name}`, label: this.state.campaign.name,
                                         shape: 'ellipse', fixed: false, size: 50}, ...node_list]);

    let edge_list = this.depboxes.map(x => ({id: x.box.id, from: x.stage1, to: x.stage2}));
    let edges = new vis.DataSet(edge_list);

    // provide the data in the vis format
    let data = {
        nodes: this.nodes,
        edges: edges
    };
    const options = {
        autoResize: true,
        physics: false,
        nodes: {
            shadow: true,
            shape: 'box'
        },
        layout: {
            improvedLayout: true,
            hierarchical: {
                enabled: true,
                levelSeparation: 150,
                nodeSpacing: 80,
                treeSpacing: 30,
                parentCentralization: true,
                blockShifting: true,
                edgeMinimization: true,
                direction: "LR",
                sortMethod: "directed"
            }
        },
        edges: {
            smooth: {
                //VP~ type: "dynamic",
                type: "discrete",
                forceDirection: "horizontal",
                roundness: 1
            },
            width: 2,
            arrows: {to : true},
            shadow: true
        },
        manipulation: {
            addNode: function (data, callback) {
                // filling in the popup DOM elements
                document.getElementById('node-operation').innerHTML = "Add Node";
                editNode(data, clearNodePopUp, callback);
            },
            //editNode: function (data, callback) {
                //// filling in the popup DOM elements
                //document.getElementById('node-operation').innerHTML = "Edit Node";
                //editNode(data, cancelNodeEdit, callback);
            //},
            deleteNode: (data, callback) => {
                const node_id = data.nodes[0];
                console.log("Deleting ", node_id);
                // Delete from DB
                //VP~ new wf_uploader().make_poms_call('campaign_edit', {'action': 'delete', 'pcl_call': '1', 'name': node_id, 'unlink': 0}, null);
                //
                // Delete stages+dependencies from state
                const l1 = Object.keys(this.state).filter(x => x.endsWith(node_id));
                console.log("Main: ", l1);
                l1.forEach(x => delete this.state[x]);
                // Delete referred dependencies
                const l2 = Object.keys(this.state).filter(x => x.startsWith("dependencies ")).filter(x => this.state[x].campaign_stage_1==node_id);
                console.log("Then: ", l2);
                l2.forEach(x => delete this.state[x]);
                // Update stage list in state
                this.state.campaign.campaign_stage_list = this.state.campaign.campaign_stage_list.split(' ').filter(x => x != node_id).join(' ');
                callback(data);
            },
            addEdge: function (data, callback) {
                if (data.from == data.to) {
                    callback(null);
                }
                else {
                    saveEdgeData(data, callback);
                }
            },
            deleteEdge: (data, callback) => {
                data.state = this.state;
                deleteEdge(data, callback);
            }
        }
    };

    // initialize network
    let container = document.getElementById('mystages');
    gui_editor.network = new vis.Network(container, data, options);

    const getLabel = e => this.nodes.get(e).label;

    gui_editor.network.on("doubleClick", function (params) {
        if (params.nodes[0] !== undefined) {
            const node = params.nodes[0];
            //VP~ const ename = node.startsWith("Default") ? `fields_${node}` : `fields_campaign_stage ${node}`;
            const ename = node.startsWith("campaign ") ? `fields_${node}` : `fields_campaign_stage ${node}`;
            const el = document.getElementById(ename);
            el.style.display = 'block';
            el.style.left = `${params.pointer.DOM.x}px`;
            el.style.top = `${params.pointer.DOM.y+20}px`;
        } else if (params.edges[0] !== undefined) {
            const edge = params.edges[0];
            const el = document.getElementById(`fields_${edge}`);
            el.style.display = 'block';
            el.style.left = `${params.pointer.DOM.x}px`;
            el.style.top = `${params.pointer.DOM.y+20}px`;
            const h3 = el.querySelector('h3');
            const e = edges.get(edge);
            h3.innerHTML = `dependency ${getLabel(e.from)} -> ${getLabel(e.to)}`;
        }
        params.event = "[original event]";
        document.getElementById('eventSpan').innerHTML = '<h2>doubleClick event:</h2>' + JSON.stringify(params, null, 4);
    });

    gui_editor.network.on("oncontext", function (params) { // right click
        this.unselectAll();
        const node = [this.getNodeAt(params.pointer.DOM)];
        if (node[0]) {
            this.selectNodes(node);
        }
        params.nodes = node;
        params.event = "[original event]";
        document.getElementById('eventSpan').innerHTML = '<h2>oncontext (right click) event:</h2>' + JSON.stringify(params, null, 4);

        if (params.nodes[0] !== undefined) {
            document.getElementById('node-operation').innerHTML = "Add Node";
            editNode(params, clearNodePopUp, addNewNode);
        }
    });

    let setup_nodes = launchtemplist.map(x => ({id: `login_setup ${x}`, label: x, shape: 'ellipse', color: '#22efcc'}));
    let jtype_nodes = this.jobtypelist.map(x => ({id: `job_type ${x}`, label: x}));

    gui_editor.aux_network = new vis.Network(document.getElementById('myjobtypes'), {
            nodes: [...setup_nodes, ...jtype_nodes],
            edges: []
        }, {
            autoResize: true,
            physics: true,
            nodes: {
                shadow: true,
                shape: 'box'
            },
            layout: {
                improvedLayout: true,
                hierarchical: {
                    enabled: true,
                    levelSeparation: 150,
                    nodeSpacing: 50,
                    parentCentralization: true,
                    blockShifting: true,
                    edgeMinimization: true,
                    direction: "UD",
                    sortMethod: "directed"
                }
            }
        }
    );

    gui_editor.aux_network.on("doubleClick", function (params) {
        if (params.nodes[0] !== undefined) {
            const node = params.nodes[0];
            const el = document.getElementById(`fields_${node}`);
            el.style.display = 'block';
            el.style.left = `${params.pointer.DOM.x}px`;
            el.style.top = `${params.pointer.DOM.y + 400}px`;
        }
        params.event = "[original event]";
        document.getElementById('eventSpan').innerHTML = '<h2>doubleClick event:</h2>' + JSON.stringify(params, null, 4);
    });

    function editNode(data, cancelAction, callback) {
        document.getElementById('node-label').value = data.label;
        document.getElementById('node-saveButton').onclick = saveNodeData.bind(this, data, callback);
        document.getElementById('node-cancelButton').onclick = cancelAction.bind(this, callback);
        document.getElementById('node-popUp').style.display = 'block';
    }

    // Callback passed as parameter is ignored
    function clearNodePopUp() {
        document.getElementById('node-saveButton').onclick = null;
        document.getElementById('node-cancelButton').onclick = null;
        document.getElementById('node-popUp').style.display = 'none';
    }

    function cancelNodeEdit(callback) {
        clearNodePopUp();
        callback(null);
    }

    const saveNodeData = (data, callback) => {
        const label = document.getElementById('node-label').value;
        clearNodePopUp();
        if (label.includes('*')) {
            const nn = label.split('*');
            for (let i = 0; i < nn[1]; i++) {
                data.label = `${nn[0]}_${i}`;
                const reply = callback(data);
                // Now handle our stuff
                this.new_stage(reply[1], data.label);
                this.add_dependency(reply[0], reply[1], reply[2]);
            }
        } else {
            data.label = label;
            const reply = callback(data);
            // Now handle our stuff
            if (data.id) {
                this.new_stage(data.id, data.label);
            } else {
                this.new_stage(reply[1], data.label);
                this.add_dependency(reply[0], reply[1], reply[2]);
            }
        }
    }

    function clearEdgePopUp() {
        document.getElementById('edge-saveButton').onclick = null;
        document.getElementById('edge-cancelButton').onclick = null;
        document.getElementById('edge-popUp').style.display = 'none';
    };

    function cancelEdgeEdit(callback) {
        clearEdgePopUp();
        callback(null);
    }

    const saveEdgeData = (data, callback) => {
        if (typeof data.to === 'object')
            data.to = data.to.id;
        if (typeof data.from === 'object')
            data.from = data.from.id;
        //data.label = document.getElementById('edge-label').value;
        //clearEdgePopUp();
        const eid = edges.add({from: data.from, to: data.to})[0];
        data.id = this.add_dependency(data.from, data.to, eid);
        //VP~ callback(data);
        callback(null);
    }

    const deleteEdge = (data, callback) => {
        const id = data.edges[0];
        const el = gui_editor.network.body.edges[id];
        const link = [el.fromId, el.toId];
        //new wf_uploader().make_poms_call('campaign_edit', {'action': 'delete', 'pcl_call': '1', 'unlink': JSON.stringify(link)}, null);
        //// delete this.state[id];
        //// this.depboxes = this.depboxes.filter(x => !(x.box.id==id));
        callback(data);
    }

    const addNewNode = (params) => {
        //// var newId = (Math.random() * 1e7).toString(32);
        let newId = params.label;
        const parentId = params.nodes[0];
        //VP~ const eid = this.add_dependency(parentId, newId);
        //VP~ const nid = this.nodes.add({id: newId, label: params.label,
        const nid = this.nodes.add({label: params.label,
                                    group: this.nodes.get(parentId).group ? this.nodes.get(parentId).group + 1 : 0})[0];
        //VP~ edges.add({id: eid, from: parentId, to: newId});
        const eid = edges.add({from: parentId, to: nid})[0];
        //VP~ this.add_dependency(parentId, nid);
        return [parentId, nid, eid];
    }

}

/*
 * redo the dependency boxes on this gui_editor
 */
gui_editor.prototype.redraw_deps = function () {
    var k;
    for (k in this.depboxes) {
        this.depboxes[k].set_bounds();
    }
}

gui_editor.prototype.make_select = function(sval, eid, placeholder) {
    /*
        <select name="carlist" form="carform">
            <option value="volvo">Volvo</option>
            <option value="saab">Saab</option>
            <option value="opel">Opel</option>
            <option value="audi">Audi</option>
        </select>
     */
        let res = this.jobtypes.reduce(
            function (acc, val) {
                const sel = (val == sval) ? ' selected' : '';
                return acc + `<option value="${val}"${sel}>${val}</option>\n`;
            },
        `<option value="" disabled selected hidden>${placeholder}</option>\n`);
        return `<select id="${eid}" name="job_type"  required>\n${res}</select>\n`;
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
    box.innerHTML = "<h2>" + text + "</h2>";
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
    var i, k, x, y;
    var stage;
    if (name == undefined) {
        /* make prototype call work... */
        return;
    }
    this.gui = gui;
    this.dict = vdict;
    this.klist = klist;
    this.box = document.createElement("DIV");
    this.box.gui_box = this;
    this.box.className = "box";
    this.box.id = name;
    //if (name.length - name.indexOf(' ') > 20) {
        //this.box.style.width = "185px";
    //} else {
        //this.box.style.width = "120px";
    //}
    const w = Math.max(...(name.split(' ').map(v => v.length))) * 8;
    this.box.style.width = Math.max(w, 128) + "px";

    this.box.style.left = x.toString() + "px";
    this.box.style.top = y.toString() + "px";
    this.box.addEventListener("click", function () { gui_editor.toggle_box_selected(name) });
    this.popup_parent = document.createElement("DIV");
    this.popup_parent.gui_box = this;
    this.popup_parent.className = "popup_parent";
    x = x + 50;
    y = y + 50;
    stage = name.substr(name.indexOf(" ") + 1)

    // Build the form...
    var val, placeholder;
    const ro = name.match(/job_type|login_setup/) ? "disabled" : "";
    let res = [];
    //res.push(`<form id="fields_${name}" class="popup_form" style="display: none; top: ${y}px; left: ${x}px;">`);
    res.push(`<form id="fields_${name}" class="popup_form" style="display: none;" data-hash="" data-clean="1">`);
    res.push('<h3>' + name.split(' ')[0]);
    //if (name != 'Default Values') {
    //    res.push(`<button title="Delete" class="rightbutton" type="button" onclick="gui_editor.delete_me('${name}')"><span class="deletebutton"></span></button><p>`);
    //}
    res.push('</h3>');
    for (i in klist) {
        k = klist[i];
        if (k.startsWith('campaign_stage'))      // Hack to hide this from dependency form
            continue;
        if (vdict[k] == null) {
            val = "";
            //VP~ placeholder = "default";
            placeholder = (this.gui.mode[k]).toString();
        } else {
            val = vdict[k];
            //VP~ placeholder = "default";
            placeholder = (this.gui.mode[k]);
        }
        res.push(`<label>${k}</label>`);
        if (k.includes("job_type")) {
            res.push(this.gui.make_select(val, `${this.get_input_tag(k)}`, placeholder));
        } else {
            res.push(`<input id="${this.get_input_tag(k)}" name="${k}" value="${this.escape_quotes(val)}" placeholder="${this.escape_quotes(placeholder)}" ${ro}>`);
        }
        if (k.indexOf('param') >= 0) {
            res.push(`<button type="button" onclick="json_field_editor.start('${this.get_input_tag(k)}')">Edit</button>`);
        }
        res.push('<br>');
    }
    //res.push(`<button class="rightbutton" type="button" onclick="this.parentElement.style.display='none';">OK</button>`);
    res.push(`<button class="rightbutton" type="button" onclick="gui_editor.toggle_form('fields_${name}')">OK</button>`);
    res.push(`<button type="reset" value="Reset">Reset</button>`);
    res.push('</form>');
    // Form is ready
    this.popup_parent.innerHTML = res.join('\n');

    top.appendChild(this.box);
    //top.appendChild(this.popup_parent);
    const pp = document.getElementById("popups");
    pp.appendChild(this.popup_parent);
    // Now calculate and store a hash
    const hval = mwm_utils.hashCode(JSON.stringify(mwm_utils.formFields(this.popup_parent)));
    $(`form[id='fields_${name}']`).attr('data-hash', hval);
}

/*
 * actual object delete code called by initial click handler
 */
generic_box.prototype.delete_me = function () {
    var i;
    var name = this.box.id;
    /* clean up circular reference... */
    this.box.gui_box = null;
    this.box.parentNode.removeChild(this.box);
    this.box = null;
    this.popup_parent.parentNode.removeChild(this.popup_parent);
    this.popup_parent = null;
    /*
     * in a perfect OO implementation we would subclass for these
     * but we can just be polymorphic and check if we have certain
     * object members...
     */
    if ('db' in this) {
        this.db.parentNode.removeChild(this.db);
        this.db = null;
    }
    if ('db2' in this) {
        this.db2.parentNode.removeChild(this.db2);
        this.db2 = null;
    }
    for (i in this.klist) {
        delete this.dict[this.klist[i]];
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

    return "_inp" + this.box.id + '_' + k;
}

/*
 * save values from a popup form back into the state.
 */
generic_box.prototype.save_values = function () {
    var inp_id, e, k, i;
    for (i in this.klist) {
        k = this.klist[i];
        e = document.getElementById(this.get_input_tag(k));
        if (e != null) {
            this.dict[k] = e.value ? e.value : null;
        } else {
            console.log('unable to find input ' + inp_id);      // FIXME: inp_id is not set
        }
    }
}

/*
 * convert quotes to &quot; for <input value="xxx'> values
 * should be a  astatic method...
 */
generic_box.prototype.escape_quotes = function(s) {
   if (s != undefined) {
       return s.replace(/"/g,'&quot;');
   } else {
       return s;
   }
}


/*
 * non-draggable, non-dependency box class
 * for job_types, etc.  subclass of generic_box
 */
function misc_box(name, vdict, klist, top, x, y, gui) {
    this.generic_box = generic_box; /* superclass init */
    this.generic_box(name, vdict, klist, top, x, y, gui);
    this.box.innerHTML = `${name}<br> <button type="button" onclick="gui_editor.toggle_form('fields_${name}')" id="wake_fields_${name}"><span class="wakefields"></span></button>` ;
}
misc_box.prototype = new generic_box();

/*
 * box to represent campaign stages -- draggable, etc.
 */
function stage_box(name, vdict, klist, top, x, y, gui) {
    this.generic_box = generic_box; /* superclass init */
    this.generic_box(name, vdict, klist, top, x, y, gui);
    this.box.draggable = true;
    this.box.addEventListener("dragstart", gui_editor.drag_handler);
    this.box.innerHTML = `${name}<br> <button type="button" onclick="gui_editor.toggle_form('fields_${name}')" id="wake_fields_${name}"><span class="wakefields"></span></button>` ;
}
stage_box.prototype = new generic_box();

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
    this.box.style.position = 'absolute';
    this.db = document.createElement("DIV");
    this.db.id = 'dep1_' + this.stage1 + '_' + this.stage2;
    this.db.className = 'depbox1';
    this.box.appendChild(this.db);
    this.db2 = document.createElement("DIV");
    this.db2.className = 'depbuttonbox2';
    this.db2.style.position = 'absolute';
    top.appendChild(this.db2);
    this.db2.innerHTML = `<button onclick="gui_editor.toggle_form('fields_${name}')" id="wake_fields_${name}"><span class="wakefields"></span></button>`;

    this.set_bounds();
}
dependency_box.prototype = new generic_box();

/*
 * routine to (re)set the bounds of a dependency box
 * so its corners are centered on the two states that
 * it involves.
 * note that this also moves the popup form for the
 * dependency to be next to it (even while hidden)
 */
dependency_box.prototype.set_bounds = function () {
    console.log("set_bounds('" + this.stage1 + "' , '" + this.stage2 + "')");
    var e1 = document.getElementById("campaign_stage " + this.stage1);
    var e2 = document.getElementById("campaign_stage " + this.stage2);
    if (e1 == null) {
        console.log("could not find campaign_stage: '" + this.stage1 + "'");
        return;
    }
    if (e2 == null) {
        console.log("could not find campaign_stage: '" + this.stage2 + "'");
        return;
    }
    var br = e1.parentNode.getBoundingClientRect();
    var e1r = e1.getBoundingClientRect();
    var e2r = e2.getBoundingClientRect();
    var x1, x2, y1, y2, midx, midy, ulx, uly, lrx, lry, w, h;

    console.log(['e1r', e1r]);
    console.log(['e2r', e2r]);
    /* just go from center of one box to the other */
    x1 = (e1r.left + e1r.right) / 2;
    x2 = (e2r.left + e2r.right) / 2;
    y1 = (e1r.bottom + e1r.top) / 2;
    y2 = (e2r.top + e2r.bottom) / 2;
    ulx = Math.min(x1, x2);
    lrx = Math.max(x1, x2);
    uly = Math.min(y1, y2);
    lry = Math.max(y1, y2);

    console.log("set_bounds(): " + x1.toString() + "," + y1.toString() + ":" + x2.toString() + "," + y2.toString());

    var midx = (x1 + x2) / 2;

    /* width and height... */
    w = lrx - ulx;
    h = lry - uly;

    var circular = (this.stage1 == this.stage2);

    if (circular) {
        w = 30;
        h = 70;
        lry += 70;
        lrx += 30;
    }

    var uphill = (y2 >= y1);

   /* make relative to bounding rectangle */
   ulx = ulx - br.left;
   uly = uly - br.top;
   lrx = lrx - br.left;
   lry = lry - br.top;
   midx = midx - br.left;

    this.box.style.top = uly.toString() + "px";
    this.box.style.left = ulx.toString() + "px";
    this.box.style.height = h.toString() + "px";
    this.box.style.width = w.toString() + "px";

    if (circular) {
        this.db.className = 'depbox1 circular';
    } else if (uphill) {
        this.db.className = 'depbox1 uphill';
    } else {
        this.db.className = 'depbox1 downhill';
    }
    this.db2.style.left = (midx - 10).toString() + "px";
    this.db2.style.top = lry.toString() + "px";
    var f = document.getElementById('fields_' + this.box.id);
    if (f != null) {
        f.style.left = (midx + 10).toString() + "px";
        f.style.top = (lry + 10).toString() + "px";
    } else {
        console.log("could not find: fields_" + this.box.id);
    }
}

gui_editor.prototype.new_stage = function (name, label) {
    var k = name || window.prompt("New stage name:");
    var x, y, b;
    this.state['campaign']['campaign_stage_list'] += " " + k;
    k = 'campaign_stage ' + k;
    this.state[k] = {
        'name': label,
        'vo_role': null,
        'state': null,
        'software_version': null,
        'dataset': null,
        'cs_split_type': null,
        'completion_type': null,
        'completion_pct': null,
        'param_overrides': null,
        'test_param_overrides': null,
        'login_setup': null,
        'job_type': null,
    };
    x = 500;
    y = 150;
    b = new stage_box(k, this.state[k], mwm_utils.dict_keys(this.state[k]), this.div, x, y, this);
    this.stageboxes.push(b);
}

gui_editor.prototype.new_dependency = function() {
    var elist1, elist, k1, istr, db, s1, s2, i;
    elist1 = this.div.getElementsByClassName('selected');
    elist = [];
    for (i = 0; i < elist1.length; i++) {
        elist.push(elist1[i]);
    }

    console.log(['selected', elist]);

    if (elist.length < 2 || elist.length > 2) {
        window.alert("Need exactly two Campagn Stages selected");
        return;
    }
    /* we think the left-to-right position on the screen indicates
     * which way the dependency should go, so if they're the other
     * way around, swap them...
     */
    if (elist[0].getBoundingClientRect().x > elist[1].getBoundingClientRect().x) {
        var t = elist[0];
        elist[0] = elist[1];
        elist[1] = t;
    }
    s1 = elist[0].id.replace("campaign_stage ", "");
    s2 = elist[1].id.replace("campaign_stage ", "");
    k1 = 'dependencies ' + s2;
    if (k1 in this.state) {
        istr = ((mwm_utils.dict_size(this.state[k1]) / 2) + 1).toString();
    } else {
        this.state[k1] = {};
        istr = '1';
    }
    console.log('elist:');
    console.log(elist);

    this.state[k1]['campaign_stage_' + istr] = s1;
    this.state[k1]['file_pattern_' + istr] = '%%';
    db = new dependency_box(k1 + "_" + istr, this.state[k1], ["campaign_stage_" + istr, "file_pattern_" + istr], this.div, 0, 0, this);
    this.depboxes.push(db);
}


gui_editor.prototype.add_dependency = function(frm, to, id) {
    let dep_name = id;
    //VP~ let dep_name = 'dependencies ' + to;
    if (dep_name in this.state) {
        var istr = ((Object.keys(this.state[dep_name]).length / 2) + 1).toString();
    } else {
        this.state[dep_name] = {};
        istr = '1';
    }

    this.state[dep_name]['campaign_stage_' + istr] = frm;
    this.state[dep_name]['file_pattern_' + istr] = '%%';
    //VP~ const db = new dependency_box(`${dep_name}_${istr}`,
    const db = new dependency_box(dep_name,
                                  this.state[dep_name],
                                  [`campaign_stage_${istr}`, `file_pattern_${istr}`],
                                  this.div, 0, 0, this);
    this.depboxes.push(db);
    return db.box.id;
}

gui_editor.prototype.save_state = function () {
    var sb = document.getElementById("savebusy");
    sb.innerHTML = "Saving...";
    /* call with setTimeout to give Saving a chance to show up */
    //VP~ window.setTimeout( () => {
        gui_editor.exportNetwork();
        sb.innerHTML = "Done.";
        gui_editor.unmodified();

        const args = mwm_utils.getSearchParams()
        const base = mwm_utils.getBaseURL()
        console.log(["args:", args, "base:", base ])
        if (args['clone'] != undefined) {
            const campaign = args['to'];
            location.href = `${base}gui_wf_edit?campaign=${campaign}`;
        } else {
            location.reload();
        }
    //VP~ }, 200);
    /*
    window.setTimeout( () => {
        var wu = new wf_uploader();
        console.log(["wu", wu]);
        this.undefaultify_state();
        //const deps = this.depboxes.map(d => [d.box.id, d.stage1, d.stage2]);
        //VP~ let cfg_stages = this.nodes.map(x => [x.id, x.group]).filter(x => !x[0].startsWith("Default")).sort( (a, b) => b[1] - a[1] ).map(x => x[0]);
        let cfg_stages = this.nodes.map(x => [x.id, x.group]).filter(x => !x[0].startsWith("campaign ")).sort( (a, b) => b[1] - a[1] ).map(x => x[0]);
        wu.upload(this.state, cfg_stages, () => {
            // callback for when whole upload is done..
            console.log("finally done uploading, whew");
            this.defaultify_state();
            sb.innerHTML = "Done.";
            gui_editor.unmodified();
        });
    }, 5);
    */
}


/*
 * ===================================================================
 * uploader -- translated from the upload_wf code in poms_client...
 */
function wf_uploader() {
    var i, s, l, jt;
    this.cfg = null;
}

wf_uploader.prototype.upload = function(state, cfg_stages, completed) {
    this.cfg = state;
    var thisx = this;
    this.get_headers(function (headers) {

        thisx.username = headers['X-Shib-Userid'];
        if (thisx.username == undefined) {
            thisx.username = 'mengel';
        }
        thisx.experiment = state['campaign']['experiment']
        var role = state['campaign']['poms_role'];
        thisx.update_session_role(role);
        //var cfg_stages = thisx.cfg['campaign']['campaign_stage_list'].split(' ');
        //var cfg_stages = Object.keys(thisx.cfg).filter(x => x.startsWith('campaign_stage '));
        //cfg_stages = cfg_stages.map(x => x.split(' ')[1]);
        var cfg_jobtypes = {};
        var cfg_launches = {};
        var i, l, jt, s;
        //
        for (i in cfg_stages) {
            s = cfg_stages[i];
            if (('campaign_stage ' + s) in thisx.cfg) {
                cfg_launches[thisx.cfg['campaign_stage ' + s]['login_setup']] = 1;
                cfg_jobtypes[thisx.cfg['campaign_stage ' + s]['job_type']] = 1;
            }
        }
        for (l in cfg_launches) {
            thisx.upload_login_setup(l);
        }
        for (jt in cfg_jobtypes) {
            thisx.upload_jobtype(jt);
        }
        /* upload3 will call upload_stage which needs the existing stage map
         * to decide whether to add or edit, so start an async fetch here.
         */
        console.log("upload get-headers callback calling get_campaign_list");
        thisx.get_campaign_list().then(
            _ => {
                console.log("calling upload2...")
                return thisx.upload2(state, cfg_stages, completed);
            }
        ).then(
            _ => {
                console.log("calling tag_em...");
                thisx.tag_em(thisx.cfg['campaign']['name'], cfg_stages, completed);
            }
        );
    });
}

wf_uploader.prototype.upload2 = function(state, cfg_stages, completed) {
    let p = Promise.resolve();
    for (const i in cfg_stages) {
        const s = cfg_stages[i];
        console.log("upload2: stage:", s);
        p = p.then(_ => this.upload_stage(s));
    }
    /*
    for (const d of this.deps) {
        console.log("upload2: dep:", d);
        p = p.then(_ => this.upload_dependency(d));
    }
    */
    return p;
}


wf_uploader.prototype.tag_em = function(name, cfg_stages, completed) {   // FIXME: Might not needed as is.
    var thisx = this;
    /* have to re-fetch the list, if we added any campaigns... */
    console.log("tag_em calling get_campaign_list")
    this.get_campaign_list(function () {
        console.log(["have campaign_id_map", thisx.cname_id_map, "stages:", cfg_stages]);
        var cim = thisx.cname_id_map;
        var cids = cfg_stages.map(function (x) { return (x in cim) ? cim[x].toString() : x });
        console.log(["have campaign_list", thisx.cname_id_map]);

        var args = { 'campaign_name': name, 'campaign_stage_id': cids.join(','), 'experiment': thisx.cfg['campaign']['experiment'] };
        thisx.make_poms_call('link_tags', args, completed);
    });
}

wf_uploader.prototype.upload_jobtype = function(jt) {
    var field_map, k, d, args;
    if (!(('job_type ' + jt) in this.cfg)) {
        return;
    }
    field_map = {
        'launch_script': 'ae_launch_script',
        'parameters': 'ae_definition_parameters',
        'output_file_patterns': 'ae_output_file_patterns',
    };
    d = this.cfg['job_type ' + jt];
    args = {
        'pcl_call': '1',
        'action': 'add',
        'ae_definition_name': jt,
        'experiment': this.cfg['campaign']['experiment'],
        'pc_username': this.username,
    };
    for (k in d) {
        if (k in field_map) {
            args[field_map[k]] = d[k];
        } else {
            args[k] = d[k];
        }
    }
     /* there are separate add/update calls; just do both, if it
      * exists already, the first will fail..
      */
    var thisx = this;
    this.make_poms_call('campaign_definition_edit', args, function() {
        args['action'] = 'edit';
        thisx.make_poms_call('campaign_definition_edit', args, null);
   });
}

wf_uploader.prototype.upload_login_setup = function (l) {
    var field_map, d, args, k;
    field_map = {
        'host': 'ae_launch_host',
        'account': 'ae_launch_account',
        'setup': 'ae_launch_setup',
    };
    if (!(('login_setup ' + l) in this.cfg)) {
        return;
    }
    d = this.cfg['login_setup ' + l];
    console.log(['d', d])
    args = {
        'action': 'add',
        'pcl_call': '1',
        'pc_username': this.username,
        'ae_launch_name': l,
        'experiment': this.cfg['campaign']['experiment']
    };
    for (k in d) {
        if (k in field_map) {
            args[field_map[k]] = d[k];
        } else {
            args[k] = d[k];
        }
    }
    var thisx = this;
    this.make_poms_call('login_setup_edit', args, function () {
        args['action'] = 'edit';
        thisx.make_poms_call('login_setup_edit', args);
    });
}

wf_uploader.prototype.upload_stage = function(stage_name) {
    var i, dst, deps, d, args, k, pat;
    const depname = `dependencies ${stage_name}`;
    const field_map = {
        'dataset': 'ae_dataset',
        'software_version': 'ae_software_version',
        'vo_role': 'ae_vo_role',
        'cs_split_type': 'ae_split_type',
        'job_type': 'ae_campaign_definition',
        'login_setup': 'ae_launch_name',
        'param_overrides': 'ae_param_overrides',
        'completion_type': 'ae_completion_type',
        'completion_pct': 'ae_completion_pct',
    };
    deps = { "file_patterns": [], "campaign_stages": [] }
    /* Disable dependencies building to decouple saving the stages from saving the dependencies */
    for (i = 0; i < 10; i++) {
        if ((depname in this.cfg) && (`campaign_stage_${i}`) in this.cfg[depname]) {
            dst = this.cfg[depname][`campaign_stage_${i}`];
            pat = this.cfg[depname][`file_pattern_${i}`];
            deps["campaign_stages"].push(dst);
            deps["file_patterns"].push(pat);
        }
    }

    d = this.cfg[`campaign_stage ${stage_name}`];
    args = {
        'pcl_call': '1',
        'pc_username': this.username,
        'action': (stage_name in this.cname_id_map) ? 'edit' : 'add',
        'ae_campaign_name': stage_name,
        'experiment': this.cfg['campaign']['experiment'],
        'ae_active': 'True',
        'ae_depends': JSON.stringify(deps),
    }
    for (k in d) {
        if (k in field_map) {
            args[field_map[k]] = d[k];
        } else {
            args[k] = d[k];
        }
    }
    return this.make_poms_call('campaign_edit', args);
}


wf_uploader.prototype.upload_dependency = function(dependency) {
    const link = dependency.slice(1);

    const args = {
        'pcl_call': '1',
        'pc_username': this.username,
        'action': 'add_dep',
        'ae_campaign_name': '',
        'experiment': this.cfg['campaign']['experiment'],
        'ae_active': 'True',
        'link': JSON.stringify(link),
    }
    // return this.make_poms_call('campaign_edit', args);
}


wf_uploader.prototype.get_campaign_list = function(completed) {
    var x, res, i, triple;
    var thisx = this;
    return this.make_poms_call('campaign_list_json', {}, function (x) {
        res = {};
        console.log(["back from campaign_list_json, x is", x]);
        for (i in x) {
            triple = x[i];
            if (triple.experiment == thisx.experiment) {
                res[triple.name] = triple.campaign_stage_id;
            }
        }
        thisx.cname_id_map = res;
        console.log(["back from get_campaign_list, cname_id_map", res]);
        if (completed) {
            completed(res);
        }
    });
}

wf_uploader.prototype.update_session_role = function (role) {
    return this.make_poms_call('update_session_role', { 'session_role': role });
}

wf_uploader.prototype.get_headers = function (after) {
    this.make_poms_call('headers', {}, function (s) {
        s = s.replace(/\'/g, '"');
        after(JSON.parse(s));
    });
}

wf_uploader.prototype.make_poms_call = function (name, args, completed) {
    var k, res;
    var base = mwm_utils.getBaseURL();
    console.log(['make_poms_call', name, args]);
    for (k in args) {
        if (args[k] == null || args[k] == undefined) {
            delete args[k];
        }
    }
    res = Promise.resolve(jQuery.ajax({
        url: base + '/' + name,
        data: args,
        method: args ? 'POST' : 'GET',
        success: function (result) {
            if (completed) {
                completed(result);
            }
        },
        error: function (result) {
            var p, resp;
            p = result.responseText.indexOf('>Traceback');
            if (p > 0) {
                resp = result.responseText.slice(p + 6);
                p = resp.indexOf('</label>')
                if (p < 0) {
                    p = resp.indexOf('</pre>');
                }
                resp = resp.slice(0, p);
                resp.replace(/<br\/>/g, '\n');
            } else {
                resp = result.responseText;
            }
            console.log(resp);
        },
        async: true,
    }));
    return res;
}

