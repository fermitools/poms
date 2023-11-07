
const daysOfWeek = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
const monthsOfYear = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
var countdownToSearch = 0;
var ongoingCountdowns = [];
var running=false;
var cancelTimestampUpdates=false;

function formatDuration(seconds) {
    var hours = Math.floor(seconds / 3600);
    var minutes = Math.floor((seconds % 3600) / 60);
    var remainingSeconds = seconds % 60;
  
    var durationParts = [];
    if (hours > 0) {
      durationParts.push(hours + ` hour` + (hours !== 1 ? `s` : ``));
    }
    if (minutes > 0) {
      durationParts.push(minutes + ` minute` + (minutes !== 1 ? `s` : ``));
    }
    if (remainingSeconds > 0 || durationParts.length === 0) {
      durationParts.push(remainingSeconds + ` second` + (remainingSeconds !== 1 ? `s` : ``));
    }
  
    return durationParts.join(`, `);
  }

function get_readable(timestamp){
    if (timestamp == null){
        return `Never`;
    }
    if (timestamp.length < 12){
        return formatDuration(timestamp);
    }
    var convertedDate = new Date(timestamp * 1000); // Convert to milliseconds
    var dayOfWeek = daysOfWeek[convertedDate.getUTCDay()];
    var month = monthsOfYear[convertedDate.getUTCMonth()];
    var day = convertedDate.getUTCDate();
    var year = convertedDate.getUTCFullYear();
    return dayOfWeek + ', ' + month + ' ' + day + ', ' + year;
}

function customReplacer(key, value) {
    console.log(`value: ` + value);
    value = value.replaceAll(`'`, '').replaceAll('`', ``).replaceAll(`}`, `<br/>}`).replaceAll(`{`, `{<br/>&emsp;`).replaceAll(`,`, `,<br/>&emsp;`);
    console.log(`new value: ` + value);
    return value;
}
function customReplacerJs(key, value) {
    if (typeof value === 'string') {
      // Remove quotes around string values and replace single quotes with escaped single quotes
      return value.replace(/^`(.*)`$/, '$1').replace(/'/g, `\\'`);
    }
    return value;
  }

function insertLineBreaks(jsonString) {
    return jsonString.replace(/(\[|\,|\])/g, '$1<br/>');
  }

function capitalizeWords(str) {
    return str.toLowerCase().split('_').map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
  }
function percentage(partialValue, totalValue) {
    return (100 * partialValue) / totalValue;
 } 
function getProjectHandles(self, project_id, change_ui=true){
    if (!change_ui){
        if ($(self).css(`background-color`) === 'rgb(255, 255, 0)' || $(self).css(`background-color`) === 'yellow'){
            $(self).css(`background-color`, ``);
            
            $(`.project_handle_label`).removeClass('dd-results-row-label-selected');
            $(`#active_project_handles`).removeClass(`dd-row-toggle-active`).addClass(`dd-row-toggle-inactive`);
                setTimeout(() => {
                    $(`#active_project_handles`).slideUp(`0.5s`, complete=function(){
                        $(`#project_handle_count`).html(`No project selected`);
                    });
            },500);
        }
        else{
            $(`.poms-dd-attribute-link`).css(`background-color`, ``);
            $(self).css(`background-color`, `yellow`);
            $(`.dd-results-card-footer`).html(`View Handles`);
            $(`.dd-results-card-footer`).removeClass(`dd-results-card-footer-active`);
            $.getJSON(pomspath +'/get_project_handles/' + session_experiment + '/' + session_role + '?project_id=' + project_id, function (data) {
                if (data.msg == `OK`) {
                    handles = data.project_handles;
                    stats = data.stats;
                    update_project_handle_view(project_id, handles, stats);
                }
            });
        }
        return;
    }
    $(`.poms-dd-attribute-link`).css(`background-color`, ``);
    if ($(self).html().includes(`View`)){
        $(`.dd-results-card-footer`).html(`View Handles`);
        $(`.dd-results-card-footer`).removeClass(`dd-results-card-footer-active`);
        $(self).html(`Hide Handles`);
        $(self).addClass(`dd-results-card-footer-active`);
        $.getJSON(pomspath +'/get_project_handles/' + session_experiment + '/' + session_role + '?project_id=' + project_id, function (data) {
            if (data.msg == `OK`) {
                handles = data.project_handles;
                stats = data.stats;
                update_project_handle_view(project_id, handles, stats);
            }
        });
    }
    else{
        $(self).removeClass(`dd-results-card-footer-active`);
        $(self).html(`View Handles`);
        if ($(`.dd-results-card-footer-active`).length == 0){
            if($(`.project_handle_label`).hasClass(`dd-results-row-label-selected`)){
                $(`.project_handle_label`).removeClass('dd-results-row-label-selected');
                $(`#active_project_handles`).removeClass(`dd-row-toggle-active`).addClass(`dd-row-toggle-inactive`);
                setTimeout(() => {
                    $(`#active_project_handles`).slideUp(`0.5s`, complete=function(){
                        $(`#active_project_handles`).html(``);
                    });
                },500);
            }
        }
        $(`#project_handle_count`).html(`No project selected`);
    }
}

function update_project_handle_view(project_id, handles, stats){
    var handleRowHtml = ``;
    for (var i = 0; i < handles.length; i++){
        var handleDetailsHtml = ``;
        var state = `initial`;
        Object.entries(handles[i]).forEach(([key, value]) => {
            if (!(key == `name` || key == `project_id` || key == null)){
                if (key == `reserved_since`){
                    handleDetailsHtml =  handleDetailsHtml + `<a>* ` + capitalizeWords(key) + `: <span class='active-project-handles-handle-` + key.replace(`_`, `-`) + `' >` + get_readable(value) + `<span></a><br/>`;
                }
                else{
                    handleDetailsHtml =  handleDetailsHtml + `<a>* ` + capitalizeWords(key) + (key==`replica` ? ` urls` : ``)  +  `: <span class='active-project-handles-handle-` + key.replace(`_`, `-`) + `' >`;
                    if (key == 'attributes'){
                        handleDetailsHtml = handleDetailsHtml + insertLineBreaks(JSON.stringify(value, customReplacerJs, 2)) + `<span></a><br/>`;
                    }
                    else if (key == 'replicas'){
                        Object.entries(value).forEach(([x,y]) => {
                            var rse_url = `<br/>&emsp; RSE: <a href='URL' target='_blank'>URL</a>`;
                            Object.entries(y).forEach(([rkey,rvalue]) => {
                                if (rkey == `rse`){
                                    rse_url = rse_url.replace(`RSE`, rvalue);
                                }
                                else if (rkey == `url`){
                                    rse_url = rse_url.replaceAll(`URL`, rvalue);
                                }
                            });
                            handleDetailsHtml = handleDetailsHtml + rse_url;
                        });
                        handleDetailsHtml = handleDetailsHtml + `<span></a><br/>`;
                    }
                    else{
                        handleDetailsHtml = handleDetailsHtml + value  + `<span></a><br/>`;
                    }
                }
            }
            if (key == `state`){
                state = value;
            }
        });
        handleRowHtml = handleRowHtml + '<div class="dd-results-card"><div class="'+state+' dd-results-card-header"><p>Name: <span class="active-project-handles-handle-name">' + handles[i].name + '</span></p></div><div class="dd-results-card-body">' + handleDetailsHtml + '</div></div>';
    }
    pct_complete = handles.length != 0 ? percentage(parseInt(stats.done) + parseInt(stats.failed), handles.length) + `%` : `N/A`;
    $(`#project_handle_count`).html(`Project Id: ` + project_id + `<span style='display:inline-block; width: 5%;'></span>Percent Complete: `+pct_complete+`<span style='display:inline-block; width: 5%;'></span>Total: ` + handles.length);
    if(!$(`.project_handle_label`).hasClass(`dd-results-row-label-selected`)){
        $(`.project_handle_label`).addClass('dd-results-row-label-selected');
    }
    if (handles.length == 0){
        $(`#active_project_handles`).removeClass(`dd-row-toggle-inactive`).addClass(`dd-row-toggle-active`);
        setTimeout(() => {
            $(`#active_project_handles`).slideUp(`0.5s`, complete=function(){
                $(`#active_project_handles`).html(handleRowHtml);
            });
        },500);
        
    }
    else{
        $(`#active_project_handles`).removeClass(`dd-row-toggle-inactive`).addClass(`dd-row-toggle-active`);
        setTimeout(() => {
            $(`#active_project_handles`).html(handleRowHtml);
            $(`#active_project_handles`).slideDown(`0.5s`);
        },500);
        
    }
}



function showLoading(){
    switch ($(`#timeTillExpire`).html()){
        case `Loading.`:
            $(`#timeTillExpire`).html(`Loading..`);
            break;
        case `Loading..`:
            $(`#timeTillExpire`).html(`Loading...`);
            break;
        case `Loading...`:
            $(`#timeTillExpire`).html(`Loading.`);
            break;
        default:
            return;
    }
    setTimeout(function() {
        showLoading();
    },500);
}

function makeElementsUniform(element){
    var maxHeightCard = Math.max.apply(null, $(element).map(function (){
        return $(this).height();
    }).get());
    var maxHeightbody = Math.max.apply(null, $(element).children(`.dd-results-card-body`).map(function (){
        return $(this).height();
    }).get());
    element.css(`height`, maxHeightCard);
    $(element).children(`.dd-results-card-body`).css(`height`, maxHeightbody);
}


function showError(message){
    $(`.dd-banner`).removeClass(`fail`).removeClass(`clicked`).removeClass(`clickable`);
    setTimeout(()=>{
        $(`.dd-banner`).html(message).addClass(`clickable`).addClass(`fail`);
    }, 250);
    cancelToken = setTimeout(()=>{
        $(`.dd-banner`).html(``).removeClass(`fail`).removeClass(`clicked`).removeClass(`clickable`);
    }, 6000);
    return cancelToken;
}

function updateTimestamp(pageStart=false){
    if (cancelTimestampUpdates){
        running = false;
        return;
    }
    if ($(`#tokenLifespan`).val() != ``){
        running = true;
        var date2 = new Date($(`#tokenLifespan`).val() * 1000);
        setTimeout(()=>{
            var date1 = new Date($.now());
            var diff = date2.getTime() - date1.getTime();
            var days = Math.floor(diff / (1000 * 60 * 60 * 24));
    
            diff -=  days * (1000 * 60 * 60 * 24);
    
            var hours = Math.floor(diff / (1000 * 60 * 60));
            diff -= hours * (1000 * 60 * 60);
    
            var mins = Math.floor(diff / (1000 * 60));
            diff -= mins * (1000 * 60);
    
            var seconds = Math.floor(diff / (1000));
            diff -= seconds * (1000);
            if (!cancelTimestampUpdates){
                $(`#timeTillExpire`).html(days + ` days : ` + hours + ` hours : ` + mins + ` minutes : ` + seconds + ` seconds`);
            }
            updateTimestamp();   
        }, pageStart?0:1000);
    }
}

function searchAndHighlight(searchBox, searchBoxes, containers, parent_row) {
    var priority = null;
    var firstMatch = null;
    var scrollTo = false;
    var totalMatches = 0;
    var logSearchCriteria = ``;
    for(var i = 0; i < containers.length; i++){
        container = containers[i];
        searchTerm = $(searchBoxes[i]).val();
        $(container).removeClass(`highlight`);
        if (searchTerm !== '') {
            logSearchCriteria += (i == 0 ? ``: `, `) + container.split(`-`).pop() + `: ` + searchTerm;
            matches = $(container + `:icontains(`+searchTerm.toString()+`)`);
            console.log(container + `: ` + searchTerm + ` - ` + matches.length + ` matches`);
            matches.addClass(`highlight`)
            totalMatches += matches.length;
            if (matches.length > 0){
                if (container.includes(searchBox)){
                    priority = matches.closest('.dd-results-card').first();
                }
                showChangesInProgress(true, searchBoxes[i]);
            }
            else{
                showChangesInProgress(false, searchBoxes[i]);
            }
        }
        else{
            showChangesInProgress(null, searchBoxes[i]);
        }
    }
    console.log(`Found ` + totalMatches + (totalMatches == 1 ? ' match' : ' matches') +' for `' + logSearchCriteria + '` ');
    if (parent_row.children().has('span.highlight').length > 0){
        scrollTo = true;
        firstMatch = parent_row.children().has('span.highlight').first();
        var counthidden = 0;
        parent_row.children().each(function() {
            // Check if the child element's children do not contain the specific class
            if (!$(this).has('span.highlight').length > 0) {
                $(this).hide();
            }
            else{
                $(this).show();
            }
        });
        if (scrollTo) {
            if (priority != null){
                console.log(`scrolling to: priority`);
                parent_row.scrollTo(priority);
            }
            else{
                console.log(`scrolling to: firstMatch`);
                parent_row.scrollTo(firstMatch.closest('.dd-results-card'));
            }
        }
    }
    else{
        parent_row.children().show();
    }
}

// Set search result indicator to green for success
// red for error
function showChangesInProgress(hadResults, searchBox){
    $('.pending').removeClass(`pending`)
    $('.success').removeClass(`success`)
    $('.error').removeClass(`error`);
    var resultIcon = $(searchBox).parent().find('.dd-search-result-icon');
    setTimeout(()=>{
        if (hadResults == null){
            console.log(`No search criteria: ` + searchBox);
        }
        else{
            if (hadResults) {
                resultIcon.addClass('success');
            } else {
                resultIcon.addClass('error');
            }
        }
    }, 50);
}


$(document).ready(function() {
    
    $(`.dd-search`).on('keyup', function(event){
        if ($(this).hasClass(`test-stand`)){
            return;
        }
        ongoingCountdowns.forEach((x)=>{
            clearTimeout(x);
        });
        var containers = [];
        var searchBoxes = [];
        var parent = null;
        var current_state = $(`#project_state_selector`).val();
        var selected_state = `projects_${current_state}`;
        // Determine where we want to search
        if (this.id.includes(`project`)){
            containers = [`.${selected_state}-project-id`, `.${selected_state}-project-owner`];
            parent = $(`#${selected_state}`);
            searchBoxes = [`#project-id`, `#project-owner`];
        }
        else if (this.id.includes(`handle`)){
            containers = [`.active-project-handles-handle-name`, `.active-project-handles-handle-namespace`, `.active-project-handles-handle-state` ];
            searchBoxes = [`#handle-name`, `#handle-namespace`, `#handle-state`];
            parent = $(`#active_project_handles`);
        }
        else if (this.id.includes(`rse`)){
            if($(`#active_rses`).hasClass(`dd-row-toggle-active`)){
                containers = [`.active-rses-rse-name`];
                parent = $(`#active_rses`);
            }
            else if($(`#inactive_rses`).hasClass(`dd-row-toggle-active`)){
                containers = [`.inactive-rses-rse-name`];
                parent = $(`#inactive_rses`);
            }
            searchBoxes = [`#rse-name`];
        }
        // In order to not kill our users browser, I implemented this countdown timer
        // which adds a status indicator in the text field the user is searching in for a 
        // 3 second interval. The countdown restarts each time a user adds a key to the
        // search criteria.
        $('.pending').removeClass(`pending`)
        $('.success').removeClass(`success`)
        $('.error').removeClass(`error`);
        if (parent == null){
            ongoingCountdowns.push(showError(`Please expand one of the lists before searching.`));
            return;
        }
        for(var i = 0; i < searchBoxes.length; i++){
            if($(searchBoxes[i]).val() !== '' && $(searchBoxes[i]).attr(`id`) != this.id){
                $(searchBoxes[i]).parent().find(`.dd-search-result-icon`).addClass(`pending`);
            }
        }
        $(this).parent().find(`.dd-search-result-icon`).addClass(`pending`);
        var timerUid = crypto.randomUUID().toString();
        countdownCancellationId = setTimeout((uid)=>{
            var searchTerms = [];
            searchAndHighlight(this.id, searchBoxes, containers, parent);
        }, 3000, timerUid);
        ongoingCountdowns.push(countdownCancellationId);
    });

    // Case insensitive contains selector for use in searchAndHighlight()
    jQuery.expr[':'].icontains = function(a, i, m) {
        return jQuery(a).text().toUpperCase()
            .indexOf(m[3].toUpperCase()) >= 0;
    };

    $("#project_state_selector").on("change", function(){

        var current_state = $(this).val();
        var selected = $(`#projects_${current_state}`);
       
        let unselected = $(".project_row.dd-row-toggle-active");
        unselected.removeClass(`dd-row-toggle-active`).addClass(`dd-row-toggle-inactive`);
        setTimeout(() => {
            unselected.slideUp(`0.5s`, complete=function(){
                selected.slideDown(`0.5s`, complete=function(){
                    selected.removeClass(`dd-row-toggle-inactive`).addClass(`dd-row-toggle-active`);
                    makeElementsUniform(selected.children(`.dd-results-card`));
                });
            });
        },500);
    });
    
    

    $('.dd-results-row-label').click(function() {
        var toggleType = $(this).hasClass('dd-toggle-active') ? 'active' : 'inactive';
        var labelType = $(this).hasClass(`project_handle_label`) ? 'project_handles' : 'rses';
        

        var toggleRowId = toggleType + '_' + labelType;
        var toggleRow = $('#' + toggleType + '_' + labelType);
        var isRseSection = (labelType == `rses`);
        var isProjectSection = (labelType == `project_handles` || labelType == `projects`);
        var handleIsBeingDisplayed = $(`.dd-results-card-footer-active`).length > 0;
        var notRowLabel = [];
        var notResultRow = [];

        // Determine which labels and rows we don't want to effect
        if (isRseSection){
            notRowLabel.push(`project_label`);
            notRowLabel.push(`project_handle_label`);
            notResultRow.push(`project_row`);
            notResultRow.push(`project_handle_row`);
        }
        else if (isProjectSection){
            var notRowLabel = [];
            var notResultRow = [];
            notRowLabel.push(`rse-label`);
            notResultRow.push(`rses_row`);
            if (labelType == `project_handles`){
                notRowLabel.push(`project_label`);
                notResultRow.push(`project_row`);
            }
            if (labelType == `projects` && handleIsBeingDisplayed){
                notRowLabel.push(`project_handle_label`);
                notResultRow.push(`project_handle_row`);
            }
        }

        // create functions to pass to jquery.not() selector
        var labelsToAvoid = function(){
            for (var i = 0; i < notRowLabel.length; i++){
                item = notRowLabel[i];
                if ($(this).hasClass(item)){
                    return true;
                }
            }
            return false;
        }
        var rowsToAvoid = function(){
            for (var i = 0; i < notResultRow.length; i++){
                item = notResultRow[i];
                if ($(this).hasClass(item)){
                    return true;
                }
            }
            return this.id == toggleRowId;
        }

        // The missile knows where it is, because it knows where it isn't.
        $('.dd-results-row-label').not($(this)).not(labelsToAvoid).removeClass('dd-results-row-label-selected');

        if ($(this).hasClass('dd-results-row-label-selected')){
            $(this).removeClass('dd-results-row-label-selected');
        }
        else{
            $(this).addClass('dd-results-row-label-selected');
        }

        
        // Toggle the visibility of the current row
        if ($(`.dd-row-toggle-active`).length > 0){
            // The missile knows where it is, because it knows where it isn't.
            $(`.dd-row-toggle-active`).not(rowsToAvoid).slideUp(`0.5s`, complete=function(){
                $(`.dd-row-toggle-active`).not(rowsToAvoid).removeClass(`dd-row-toggle-active`).addClass(`dd-row-toggle-inactive`);
            });
        }
        
        if (toggleRow.hasClass(`dd-row-toggle-inactive`)){
            
            
                toggleRow.slideDown(`0.5s`, complete=function(){
                    toggleRow.removeClass(`dd-row-toggle-inactive`).addClass(`dd-row-toggle-active`);
                    setTimeout(() => {
                        makeElementsUniform(toggleRow.children(`.dd-results-card`));
                    },500);
                });
            
            
        }
        else{
            toggleRow.removeClass(`dd-row-toggle-active`).addClass(`dd-row-toggle-inactive`);
            setTimeout(() => {
                toggleRow.slideUp(`0.5s`);
            },500);
        }
    });

    $(`.login_submit`).on(`click`, async function(e){
        e.preventDefault();
        await $.ajax({
            url: `${pomspath}/login_data_dispatcher`,
            type: 'GET',
            dataType: 'json',
            success: function(data){
                data = JSON.parse(data);
                if (data != null){
                    if (data.login_status != null){
                        $(`#login_status`).html(data.login_status);
                        if (data.login_status != `Logged in`){
                            cancelTimestampUpdates=true;
                            running=false;
                            $(`#login_method`).html(`N/A`);
                            $(`#signed-in-user`).html(`N/A`);
                            $(`#experiment`).html(`N/A`);
                            $(`#tokenLifespan`).val(``);
                            $(`#timeTillExpire`).html(`N/A`);
                        }
                    }
                    if(data.login_method != null){
                        $(`#login_method`).html(data.login_method);
                    }
                    if (data.dd_username != null){
                        $(`#signed-in-user`).html(data.dd_username);
                    }
                    if (data.dd_experiment != null){
                        $(`#experiment`).html(data.dd_experiment);
                    }
                    if (data.timestamp != null){
                        $(`#timeTillExpire`).html(`Loading.`);
                        showLoading();
                        cancelTimestampUpdates=true;
                        setTimeout(function() { 
                            $(`#tokenLifespan`).val(data.timestamp);
                            cancelTimestampUpdates=false;
                            updateTimestamp();
                        }, 2000);
                    }
                }
            },
        });
    });

    
    
    $(`.dd-banner`).on(`click`, function(){
        if(!$(`.dd-banner`).hasClass(`clickable`)){
            return;
        }
        $(this).addClass(`clicked`);
        return setTimeout(()=>{
            $(`.dd-banner`).removeClass(`fail`).removeClass(`clicked`).removeClass(`clickable`);
        }, 2000);
    });

    $(`#project_change_submit`).on(`click`, function() {
        $('.pending').removeClass(`pending`)
        $('.success').removeClass(`success`)
        $('.error').removeClass(`error`);
        $(`#test_pid`).parent().find(`.dd-search-result-icon`).addClass(`pending`);
        $(`#test_n_pass`).parent().find(`.dd-search-result-icon`).addClass(`pending`);
        $(`#test_n_fail`).parent().find(`.dd-search-result-icon`).addClass(`pending`);
        $(`#project_change_submit`).attr(`disabled`, `disabled`);
        $(`#project_restart`).attr(`disabled`, `disabled`);
        $.ajax({
            url: pomspath + `/test_project_changes/` + session_experiment + '/' + session_role + '/' + username,
            type: 'POST',
            data: {
                "project_id": parseInt($(`#test_pid`).val()),
                "n_pass": parseInt($(`#test_n_pass`).val()),
                "n_fail": parseInt($(`#test_n_fail`).val())
            },
            dataType: 'json',
            success: function(resp){
                if (resp.task_id != null) {
                    console.log(`Task: ` + resp.task_id + ` | Started`);
                    ping_until_complete(parseInt($(`#test_pid`).val()), resp.task_id, resp.start);
                }
            },
            error:(err)=>{
                $(`#project_change_submit`).removeAttr(`disabled`);
                $(`#project_restart`).removeAttr(`disabled`);
                showChangesInProgress(false, `#test_pid`);
                showChangesInProgress(false, `#test_n_pass`);
                showChangesInProgress(false, `#test_n_fail`);
            }
        });
    });
    async function ping_until_complete(project_id, task_id, started){
        setTimeout(async function(){
            data = await ping_task(project_id, task_id, started);
            if (data != null && data.status != null){
                if (data.status.running){
                    update_project_handle_view(project_id, data.info.project_handles, data.info.stats);
                    ping_until_complete(project_id, task_id, started);
                }
                else{
                    update_project_handle_view(project_id, data.info.project_handles, data.info.stats);
                    $(`#project_change_submit`).removeAttr(`disabled`);
                    $(`#project_restart`).removeAttr(`disabled`);
                    showChangesInProgress(true, `#test_pid`);
                    showChangesInProgress(true, `#test_n_pass`);
                    showChangesInProgress(true, `#test_n_fail`);
                    console.log(`Changes Complete`);
                }
            }
        }, 3000);
    }
    async function ping_task(project_id, task_id, started){
        response = null;
        await $.ajax({
            url: pomspath + `/ping_project_changes_results/` + session_experiment + '/' + session_role + '/' + username,
            type: 'POST',
            data: {
                "project_id": project_id,
                "started": parseFloat(started),
                "task_id": task_id
            },
            dataType: 'json',
            success: (resp) => {
                response =  resp;
            },
            error: function(xhr) {
                $(`#project_change_submit`).removeAttr(`disabled`);
                $(`#project_restart`).removeAttr(`disabled`);
                showChangesInProgress(false, `#test_pid`);
                showChangesInProgress(false, `#test_n_pass`);
                showChangesInProgress(false, `#test_n_fail`);
                response = xhr;
            }
        });
        return response;
    }

    $(`#project_restart`).on(`click`, function() {
        $('.pending').removeClass(`pending`)
        $('.success').removeClass(`success`)
        $('.error').removeClass(`error`);
        $(`#project_change_submit`).attr(`disabled`, `disabled`);
        $(`#project_restart`).attr(`disabled`, `disabled`);
        $(`#test_pid`).parent().find(`.dd-search-result-icon`).addClass(`pending`);
        $.ajax({
            url: pomspath + `/restart_project/` + session_experiment + '/' + session_role + '/' + username,
            type: 'POST',
            data: {
                "project_id": $(`#test_pid`).val()
            },
            dataType: 'json',
            success: (data) => {
                update_project_handle_view(parseInt($(`#test_pid`).val()), data.project_handles, data.stats);
                console.log(`Restarted Project: ` + $(`#test_pid`).val());
                $(`#project_change_submit`).removeAttr(`disabled`);
                $(`#project_restart`).removeAttr(`disabled`);
                showChangesInProgress(true, `#test_pid`);
            },
            error: (err) =>{
                showChangesInProgress(false, `#test_pid`);
                $(`#project_change_submit`).removeAttr(`disabled`);
                $(`#project_restart`).removeAttr(`disabled`);
            }
        });
    });
});

updateTimestamp(true);