"use strict";
function data_handling_config() {
    /* constructor does nothing... */
    this.default_settings = data_handling_config.default_settings;
    this.init = data_handling_config.init;
    this.render = data_handling_config.render;
    this.generate_selector = data_handling_config.generate_selector;
    this.reset_fields = data_handling_config.reset_fields;
    this.accept_changes = data_handling_config.accept_changes;
    this.save_state = data_handling_config.save_state;
    this.change_selector_value = data_handling_config.change_selector_value;
    this.generate_field = data_handling_config.generate_field;
    this.push_to = data_handling_config.push_to;
    this.generate_fields = data_handling_config.generate_fields;
    ;
}

data_handling_config.default_settings = {
    "default_service": "sam",
    "sam": {
        "fields":  [
            {
                "id": `sam_dataset`,
                "type": "text",
                "title": "SAM dataset or split data",
                "description": "Sam dataset or split data to use for this campaign stage.",
                "placeholder": null,
                "default_value": "",
            }
        ],
        "mappings": {
            "sam_dataset": "dataset_or_split_data"
        }

    },
    "data_dispatcher": {
        "fields":  [
            {
                "id": "project_id",
                "type": "number",
                "title": "Project ID",
                "description": "Numerical value of existing project id. If this value is set, Dataset Query is ignored.",
                "placeholder": null,
                "default_value": null,
            },
            {
                "id": "dataset_query",
                "type": "text",
                "title": "Dataset Query",
                "description": "MQL query for fetching project files. Creates a new project with matching files at submission time (See 'Stage Methodology').",
                "placeholder": "files from namespace:name",
                "default_value": null,
            },
            {
                "id": "idle_timeout",
                "type": "number",
                "title": "Idle Timeout",
                "description": "If there is no file reserve/release activity for the specified time interval, the project goes into 'abandoned' state. Default is 72 hours (3 days). If set to 0 or null, the project remains active until complete. Measured in seconds.",
                "placeholder": null,
                "default_value": 259200,
            },
            {
                "id": "worker_timeout",
                "type": "number",
                "title": "Worker Timeout",
                "description": "If not 0 or null, all file handles will be automatically released if allocated by same worker for longer than the `worker_timeout`. Measured in seconds.",
                "placeholder": null,
                "default_value": null,
            },
            {
                "id": "load_limit",
                "type": "number",
                "title": "Load Limit",
                "description": "The maximum number of files within a project to process per submission. 0 or blank implies no limit.",
                "placeholder": null,
                "default_value": null,
            },
            {
                "id": "virtual",
                "type": "checkbox",
                "title": "Virtual",
                "description": "Set this to true if files within your project are not physically stored somewhere.",
                "placeholder": null,
                "default_value": false,
            },
            {
                "id": "stage_methodology",
                "type": "select",
                "title": "Stage Methodology",
                "description": "Determines how POMS should handle the creation of new data dispatcher projects. This only applies if a Dataset Query is used.",
                "placeholder": null,
                "default_value": "standard",
                "options": [
                    {
                        "type": "option",
                        "text": "Standard",
                        "value": "standard",
                        "description": "POMS will create a new data dispatcher project using the provided dataset query on each campaign stage submission."
                    },
                    {
                        "type": "option",
                        "text": "OneP",
                        "value": "1P",
                        "description": "POMS will create a new data dispatcher project with the provided dataset query during the FIRST submission of the campaign stage only. All subsequent submissions of this campaign stage will use this project, unless the query is changed."
                    }
                ]
            },
            {
                "id": "recovery_mode",
                "type": "select",
                "title": "Recovery Mode",
                "description": "Specified how POMS should behave regarding recovery jobs.",
                "placeholder": null,
                "default_value": "standard",
                "options": [
                    {
                        "type": "option",
                        "text": "Standard",
                        "value": "standard",
                        "description": "POMS will automatically run recovery submission up to a maximum of 2 times, as needed. It will then continue with dependency launches, or subsequent stages."
                    },
                    {
                        "type": "option",
                        "text": "Aggressive",
                        "value": "aggressive",
                        "description": "POMS will continuously run automatic recovery submissions until the project completion percentage matches specified value, or 100%. Dependency submissions, and subsequent stages will not be launched until this threshold is met."
                    }
                ]
            }
        ],
        "mappings": {
            "project_id": "data_dispatcher_project_id",
            "worker_timeout": "data_dispatcher_worker_timeout",
            "idle_timeout": "data_dispatcher_idle_timeout",
            "dataset_query": "data_dispatcher_dataset_query",
            "virtual": "data_dispatcher_project_virtual",
            "stage_methodology": "data_dispatcher_stage_methodology",
            "recovery_mode": "data_dispatcher_recovery_mode",
            "load_limit": "data_dispatcher_load_limit",
        },
    }
}
data_handling_config.init = function(type, name=null,id = null, data=null, parent_id=null){
    this.type = type.replace(" ", "_"); // "campaign" or "campaign_stage"
    this.name = name;
    this.campaign_id = id;
    this.parent_id = parent_id;
    this.default_settings = data_handling_config.default_settings;
    this.form = document.createElement("FORM");
    if (this.type == "campaign_stage"){
        this.id = this.name.replace(" ", "_");
        this.form.id = `data_handling_service_config_editor_${this.id}`;
        this.service = data? data["currently_using"]: document.getElementById(parent_id).name === "sam_settings" ? "sam": "data_dispatcher";
        console.log(this.service);
        console.log(JSON.stringify(data));
        data = data? data :JSON.parse(document.getElementById(parent_id).value);
        this.data = {
            "name":`${name}`,
            "campaign_id": `${id}`,
            "currently_using": `${this.service}`,
            "defaults": {
                "sam": {
                    "dataset_or_split_data": data["stage_data"]["dataset_or_split_data"]
                },
                "data_dispatcher": {
                    "data_dispatcher_idle_timeout": data["stage_data"]["idle_timeout"],
                    "data_dispatcher_worker_timeout":  data["stage_data"]["worker_timeout"],
                    "data_dispatcher_project_id":  data["stage_data"]["project_id"],
                    "data_dispatcher_dataset_query":  data["stage_data"]["dataset_query"],
                    "data_dispatcher_project_virtual":  data["stage_data"]["virtual"],
                    "data_dispatcher_stage_methodology":  data["stage_data"]["stage_methodology"],
                    "data_dispatcher_recovery_mode":  data["stage_data"]["recovery_mode"],
                    "data_dispatcher_load_limit": data["stage_data"]["load_limit"]
                }
            }
        }
    }
    else{
        this.id = id;
        this.form.id = `data_handling_service_editor_${this.name}`;
        this.data = data? data : {
            "name":`${name}`,
            "campaign_id": `${id}`,
            "currently_using": 'sam',
            "defaults": {
                "sam": {
                    "dataset_or_split_data": ""
                },
                "data_dispatcher": {
                    "data_dispatcher_idle_timeout": 259200,
                    "data_dispatcher_worker_timeout": null,
                    "data_dispatcher_project_id": null,
                    "data_dispatcher_dataset_query": null,
                    "data_dispatcher_project_virtual": false,
                    "data_dispatcher_stage_methodology": "standard",
                    "data_dispatcher_recovery_mode": "standard",
                    "data_dispatcher_load_limit": null
                }
            }
        }
        this.service = this.data.currently_using? this.data.currently_using : this.default_settings.default_service; // "sam" or "data_dispatcher"
    }
    
    this.defaults = this.data["defaults"][this.service];
    
    this.form.className = "popup_form_json";
    this.form.style.top = '-10em';
    this.form.style.left = '1em';
    this.form.style.width = '50em';
    this.form.style.position = 'absolute';
    this.selector_id = `data_handling_service_selector`;

    this.builder = [];
    var mappings = this.default_settings[this.service]["mappings"];
    this.id_map = {}
    for (var [key, value] of Object.entries(mappings)){
        if (this.defaults[value] === "None" || this.defaults[value] === "null"){
            this.defaults[value] = '';
        }
        this.id_map[`${this.type}_${this.id}_${key}`] = this.defaults[value];
        console.log(this.id_map);
    }
    this.handlers_set = false;
    this.state = null;
    this.modified_values = [];
}
        

        
        
data_handling_config.render = function() {
    if (this.type == "campaign"){
        this.builder.push("<h4>Data Handling Service Editor</h4>");
    
        this.builder.push(`<table>
                <tr>
                    <td>
                        <b>SAM (Default): </b>
                    </td>
                    <td>
                        The traditional data handling service you know and love. Select this to run jobs using a SAM dataset.
                    </td>
                </tr>
                <tr>
                    <td>
                        <b>Data Dispatcher: </b>
                    </td>
                    <td>
                        <span>Select this if your campaigns are utilizing Rucio, Metacat, and Data Dispatcher (Shrek). </span>
                    </td>
                </tr>
            </table>
            <br/>`
        );
        this.generate_selector();
    }
    else{
        if (this.service === "sam"){
            this.builder.push(`<h4>SAM Settings: ${this.name}</h4>`);
        }
        else{
            this.builder.push(`<h4>Data Dispatcher Settings: ${this.name}</h4>`);
        }
        this.builder.push("<br/>");
    }
    this.generate_fields();
    this.builder.push("<br/>");
    this.builder.push(`<button type="button" id="${this.type}_service_handling_editor_cancel_changes" onclick="data_handling_service_editor.reset_fields()" class="rightbutton ui button deny red" >Cancel</button>`);
    this.builder.push(`<button type="button" id="${this.type}_service_handling_editor_accept_changes" onclick="data_handling_service_editor.accept_changes()" class="rightbutton ui button approve teal">Accept</button>`);
    this.form.innerHTML += this.builder.join('\n');
}

data_handling_config.generate_selector = function() {
            this.builder.push('<div class="form-field">');
            this.builder.push(`<label for="${this.selector_id}">Select Data Handling Service</label>`);
            this.builder.push(`<select id="${this.selector_id}" name="${this.selector_id}" value=${this.service} onChange="data_handling_service_editor.change_selector_value('${this.selector_id}')">`);
            this.builder.push(`<option value="sam" ${this.service === 'sam'? 'selected': ''} >SAM</option>`);
            this.builder.push(`<option value="data_dispatcher" ${this.service === 'data_dispatcher'? 'selected': ''}>Data Dispatcher</option>`);
            this.builder.push("</select>");
            this.builder.push('</div>');
}

data_handling_config.generate_fields =  function(reset=false) {
    var fields = this.default_settings[this.service? this.service : "sam"]["fields"];
    
    if (reset){
        let new_fields = [];
        Object.values(fields).forEach((field) => {
            var name = `${this.type}_${this.id}_${field}`;
            new_fields = this.generate_field(field, this.id_map[name], new_fields);
        });
        return new_fields;
    }
    else{
        this.builder.push('<div class="data" id="data_handling_service_selector_data">');
        Object.values(fields).forEach((field) => {
            var name = `${this.type}_${this.id}_${field}`;
            this.generate_field(field, this.id_map[name]);
        });
        this.builder.push('</div>');
    }
    
}

data_handling_config.push_to = function(str, array=null) {
    if(array){
        array.push(str);
    }
    else{
        this.builder.push(str);
    }
}
        

data_handling_config.generate_field = function(data, value = null, use_array=null) {
    var name = `${this.type}_${this.id}_${data["id"]}`;
    let existingVal = null;
    if (name in this.id_map){
        existingVal = this.id_map[name];
    }
    if (["text", "number", "checkbox"].includes(data.type)) {
        var placeholder = Object.values(data).includes("placeholder") ? data["placeholder"] : null;
        var value = Object.values(data).includes("default_value") ? data["default_value"] : null;
        if (!existingVal){
            existingVal = value;
        }
        this.push_to(`<div class="form-field">`, use_array);
        this.push_to(`<label for="${name}">${data["title"]}<i style="float: none" class="icon small help circle link tooltip" title="${data.description}"></i></label>`, use_array);
        if(data.type === "checkbox" && existingVal){
            this.push_to(`<input type="${data.type}" id="${name}" placeholder="${placeholder}" checked name="${name}" />`, use_array);
        }
        else{
            this.push_to(`<input type="${data.type}" id="${name}" placeholder="${data.placeholder}" value="${existingVal ? existingVal:value?value:''}" name="${name}" />`, use_array);
        }
        
        this.push_to('</div>', use_array);
    }
    else if (data.type  === "select") {
        var name = `${this.type}_${this.id}_${data["id"]}`;
        var placeholder = Object.values(data).includes("placeholder") ? data["placeholder"] : null;
        var selected = existingVal? existingVal: data["default_value"];
        this.push_to(`<div class="form-field">`, use_array);
        this.push_to(`<label for="${name}">${data["title"]}<i style="float: none" class="icon small help circle link tooltip" title="${data.description}"></i></label>`, use_array);
        this.push_to(`<select id="${name}"  placeholder="${data.placeholder}"  name="${name}" onChange="data_handling_service_editor.change_selector_value('${name}')">`, use_array);
        
        Object.values(data.options).forEach((field) => {
            this.push_to(`<option value="${field.value}" ${selected == field.value? "selected": ""}>${field.text}</option>`, use_array);
        });
        
        this.push_to('</select>', use_array);
        this.push_to('</div>', use_array);
    }
    if (use_array){
        return use_array;
    }
}

data_handling_config.change_selector_value = function(id) {

    var element = document.getElementById(id);
    if (element){
        this.id_map[id] = element.value;
        this.modified_values.push(id);
        if (id === "data_handling_service_selector"){
            this.service = element.value;
            this.defaults = this.data["defaults"][this.service];
            var newOptions = this.generate_fields(true);
            document.getElementById("data_handling_service_selector_data").innerHTML = newOptions.join('\n');
        }
    }
}

data_handling_config.save_state = async function() {
    console.log("Saving State");
    let requestBody = {
        "type": this.type,
        "id": parseInt(this.campaign_id),
        "service": this.service
    };
    if (this.type == "campaign_stage"){
        requestBody["stage"] = this.name;
    }
    const prefix = `${this.type}_${this.id}`;
    let new_data = {};
    Object.entries(data_handling_config.default_settings[this.service]["mappings"]).forEach(([key, value]) => {
        var element_id = `${prefix}_${key}`;
        var element = document.getElementById(element_id);
        if (element){
            if (element.type === "checkbox"){
                requestBody[value] = element.checked;
                new_data[value.replace("data_dispatcher_project_", "")] = requestBody[value]
            }
            else{
                if (element.type === "number"){
                    var numeric = parseInt(element.value);
                    if (isNaN(numeric)){
                        if (element.name.includes("project_id")){
                            numeric = null;
                        }
                        else if (element.name.includes("idle_timeout")){
                            numeric = 259200;
                        }
                        else{
                            numeric = 0;
                        }
                    }
                    requestBody[value] = numeric;  // Assign 0 if NaN, otherwise assign numeric value
                }
                else {
                    requestBody[value] = element.value;
                }
                new_data[value.replace("data_dispatcher_", "")] = requestBody[value]
            }
        }
    });
    var parent = document.getElementById(this.parent_id);
    if (parent && this.type == "campaign_stage"){
        parent.value = JSON.stringify(new_data);
    }
    else if (this.type == "campaign"){
        parent.value = this.service;
    }
    let response =  await new wf_uploader().make_poms_call('update_data_handling_service', requestBody);
}

data_handling_config.accept_changes = function() {
    var new_data = {};
    new_data.data_handling_service = this.service;
    this.save_state();
    this.modified_values = [];
    document.getElementById(this.form.id).remove();
}

data_handling_config.reset_fields = function() {
    document.getElementById(this.form.id).remove();
}



  



