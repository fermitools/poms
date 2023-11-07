
$(document).ready(function(){
    var currentUrl = window.location.href;
    var offset = 0;

    function getCookie(name) {
        var nameEQ = name + "=";
        var ca = document.cookie.split(';');
        for (var i = 0; i < ca.length; i++) {
            var c = ca[i];
            while (c.charAt(0) === ' ') c = c.substring(1, c.length);
            if (c.indexOf(nameEQ) === 0){
                console.log(`Got Cookie: ${c.substring(nameEQ.length, c.length)}`);
                return c.substring(nameEQ.length, c.length);
            }
        }
        console.log(`No Cookie`);
        return null;
    }
    offset = getCookie('position');

    

    function handleResize() {
        $("nav").css({
            "margin-top": `0px`,
            "height": `${$(window).height() -2}px`,
            "width": $("nav").width()
        });
        $("#toc .container").css({
            "height": `${$("nav").height() - $("#toc .navigation-area").height() -  $("#toc center").height() -25}px`,
        });
        $(".wrapper").css({"left": `${$("nav").width() + 25}px`})
        var margin_right = ($(window).width() * 0.1 > 150) ? ($(window).width() * 0.1) : 150;
        $(".post, .breadcrumbs").css({"width": `${$(window).width() - $("nav").width() - margin_right}px`});
        $(".back-to-top-button").css({"left": `${$(".post").pageX + (margin_right/2)}px`});
    }
    
    handleResize();
    $(window).on("resize", handleResize);
    $(document).on("scroll", function(){
        if ($(document).scrollTop() > 1000){
            $("#back-to-top-button").show();
        }
        else{
            $("#back-to-top-button").hide();
        }
    });
    $("#back-to-top-button").on("click", function(){
        var scrollTime = $(document).scrollTop() < 1000 ? $(document).scrollTop(): 1000;
        $('html, body').animate({
            scrollTop: 0
        }, scrollTime); 
    });
    $('a[href^="#"]').on('click', function(e) {
        e.preventDefault();
        const targetId = $(this).attr('href').substring(1);
        const targetElement = $('#' + targetId);
        var distance = Math.abs(targetElement.offset().top - $(document).scrollTop());
        var scrollTime = distance < 1000 ? distance : 1000;
        if (targetElement.length) {
            $('html, body').animate({
                scrollTop: targetElement.offset().top
            }, scrollTime); 
        }
    });
    function setCookie(name, value, days) {
        var expires = "";
        if (days) {
            var date = new Date();
            date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
            expires = "; expires=" + date.toUTCString();
        }
        document.cookie = name + "=" + (value || "") + expires + "; path=/";
    }
    
    function navigate(href){
        if (href != null){
            var container_Offset = $('.container').scrollTop();
            setCookie('position', container_Offset, 1);
            window.location.href = href;
        }
    }
    $("a").on("click", function(e){
        var href=$(this).attr("href");
        if ($(this).parent("li").hasClass("has-submenu")){
            var afterOffset = $(this).offset().left + $(this).width();
            if (e.pageX > afterOffset) {
                e.preventDefault();
                $(this).toggleClass("active");
                $(this).toggleClass("open");
                $(this).next("ul").slideToggle(500);
            }
            else{
                navigate(href);
            }
        }
        else{
            navigate(href);
        }
    });
    function resetCrumbs(){
        var path = window.location.pathname;
        if(!path.includes("index.html")){
            if (path.includes("?")){
                path = path.split("?")[0];
            }
            path = `${path}index.html`
            
        }
        console.log(`Path: ${path}`);
        path = path.replace(/^\/|\/$/g, '');
        var crumbs = path.split('/');
        var breadcrumbsHtml = '<a href="/docs"> POMS Guide </a>';
        var currentPath = '/docs';
        $.each(crumbs, function(index, crumb) {
            if(crumb !== '' && crumb !== 'docs') {
                console.log(`Crumb: ${crumb}`);
                currentPath += '/' + crumb;
                var words = crumb.split('_');
                var capitalizedWords = words.map(word => word.charAt(0).toUpperCase() + word.slice(1));
                var displayCrumb = capitalizedWords.join(' ');
                console.log(`displayCrumb: ${displayCrumb}`);
                if (!displayCrumb.includes(".html")){
                    breadcrumbsHtml += `<a class="separator"> | </a><a class="crumb" href="${currentPath}/index.html"> ${displayCrumb} </a>`;
                }
            }
        });
        $('.breadcrumbs').html(breadcrumbsHtml);
        $('.crumb').last().css("color", "black").attr("href", null);
    }
  

    function highlightSearchResults(query) {
        $('.highlight').removeClass('highlight');
        if (query.length > 0) {
            $('.menu-link').each(function() {
                if ($(this).text().toLowerCase().includes(query.toLowerCase())) {
                    $(this).parent().addClass('highlight');
                }
            });
        }
    }
    $('.search-icon').click(function() {
        window.location.href = `/search?query=${encodeURIComponent($('#search').val())}`;
    });

    function searchPosts(query) {
        query = query;
        var iter = 0;
        $('.searchable-post').each(function() {
            var searchText = $(this).data('search-text').split("-----");
            var title = "";
            var body = "";
            if (searchText.length > 1){
                title = searchText[0];
                body = searchText[1];
            }
            var index = body.indexOf(query);
            if (index !== -1) {
                //var excerpt = "";
                //var paragraphs = body.split('\n\n'); 
                //$.each(paragraphs, function(i, para) {
                //    if(para.indexOf(query) !== -1) {
                //        excerpt = para;
                //        return false; // Break the loop
                //    }
                //});
                //if (excerpt !== ''){
                //    var link = `${$(this).find(".search-result-page-title").attr("href")}?goto=${encodeURIComponent(query)}`
                //    excerpt = `<p>${excerpt}</p>`.replaceAll(query, `<a class="highlight" href="${link}">${query}</a>`);
                //    $(this).find(".card-body").html(excerpt);
                //}
                var i = 0;
                var proximity = 240 + query.length;
                var regex = new RegExp(query, 'gi');
                var excerpts = createExcerpts(body, query, regex, proximity);
                var $this = $(this);
                excerpts.forEach(function(excerpt) {
                    var link = `${$this.find(".search-result-page-title").attr("href")}?goto=${encodeURIComponent(query)}&match=${i}`;
                    excerpt = `<li><p>${excerpt}</p></li>`.replaceAll(regex, `<a class="highlight" href="${link}">${query}</a>`);
                    $this.find(".search-instances").append(excerpt);
                    i += 1;
                }); 
                $(this).show();
            } else {
                $(this).hide();
            }
        });
    }
    
    function createExcerpts(body, query, regex, proximity) {
        var matches = [];
        var match;
    
        // Find all match positions
        while ((match = regex.exec(body)) !== null) {
            matches.push(match.index);
        }
    
        // Group nearby occurrences
        console.log(matches);
        var groups = [];
        var currentGroup = [];
        matches.forEach(function(matchIndex, i) {
            currentGroup.push(matchIndex);
            var nextMatchIndex = matches[i + 1];
    
            // If the next match is beyond the proximity threshold, start a new group
            console.log(groups);
            if (!nextMatchIndex || nextMatchIndex - matchIndex > proximity) {
                groups.push(currentGroup);
                currentGroup = [];
            }
        });
    
        // Create excerpts for each group
        var excerpts = groups.map(function(group) {
            var start = Math.max(0, group[0] - 120);
            var end = Math.min(body.length, group[group.length - 1] + 120 + query.length);
            return "..." + body.substring(start, end) + '...';
        });
    
        return excerpts;
    }
    
    $('#search').on('keyup', function(e) {
        var query = $(this).val();
        highlightSearchResults(query);
        if (e.key === 'Enter') {
            window.location.href = `/search?query=${encodeURIComponent(query)}`;
        }
    });

    function replaceTextInElement(element, searchText) {
        element.contents().each(function () {
            if (this.nodeType === 3) { // Node type 3 is a text node
                var count = $(".goto").length;
                console.log(this.nodeValue);
                var replacedText = this.nodeValue.replaceAll(searchText, `<span class="goto match_${count}">${searchText}</span>`);
                $(this).replaceWith(replacedText);
            } else if (this.nodeType === 1) { // Node type 1 is an element
                replaceTextInElement($(this), searchText); // Recurse into children of this element
            }
        });
    }
    

    function scrollToText(text, index=null) {
        replaceTextInElement($(".post-content"), text);
        var $content = $(`.match_${index}`);
        var distance =$content.offset().top; 
        if ($content.length) {
            $('html, body').animate({
                scrollTop: distance -50
            }, 500); 
        } else {
            console.log('Text not found on the page.');
        }
    }

    if(currentUrl.includes("search")){
        var queryParams = new URLSearchParams(window.location.search);
        var query = queryParams.get('query');
        $("#query_string").html(`Search Results For: ${query}`);
        searchPosts(query);
    }
    else if(currentUrl.includes("goto")){
        var queryParams = new URLSearchParams(window.location.search);
        var goto = queryParams.get('goto');
        var idx = queryParams.get("match");
        scrollToText(goto, idx);
    }
    resetCrumbs();
    if(!currentUrl.includes("index.html")){
        if (currentUrl.includes("?")){
            currentUrl = currentUrl.split("?")[0];
        }
        currentUrl = `${currentUrl}index.html`
    }
    $(".menu-link").each(function(){
        var linkUrl = $(this).attr('href');
        if (currentUrl === linkUrl) {
            $(this).parent('.menu-item').addClass('active');
            $(this).parent('.menu-item').parents("li").addClass('active');
        }
    });
    if (offset > 0){
        setTimeout(()=>{$('.container').scrollTop(offset);}, 50);
    }
    $(".wrapper").show();
});

      