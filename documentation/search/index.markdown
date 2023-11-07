---
layout: page
title: Search
---

<div id="search-results">
    <h3 id="query_string"></h3>
    <ul>
        {% for item in site.pages %}
            {% if item.title != "Search" %}
                <li class="searchable-post" data-search-text="{{ item.title | escape_once }}-----{{ item.content | strip_html | escape_once }}">
                    <div class="card">
                        <div class="card-header search-results-header">
                            <h4>
                                <a class="search-result-page-title" href="{{ item.url }}">{{ item.title }}</a>
                            </h4>
                        </div>
                        <div class="card-body">
                            <ul class="search-instances">
                            </ul>
                        </div>
                    </div>
                </li>
            {% endif %}
        {% endfor %}
    </ul>
</div>

