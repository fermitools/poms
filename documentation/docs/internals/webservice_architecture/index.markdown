---
layout: page
title: Webservice Architecture
---
* TOC
{:toc}
POMS provides users with web access through an Apache front which handles all security. Users are validated via the lab's Shibboleth server with valid requests passed to an internal web server. This internal web server is only reachable through the Apache front end. The general architecture can be seen in the picture below.

![High Level]({{ site.url }}/docs/images/poms_overview.png)

As shown, web services are provided through a single application based on Cherrypy. Embodied with this application are the following products and services.

* A [PostgreSQL](https://www.postgresql.org/) database named pomsprd
* [SQLAlchemy](http://www.sqlalchemy.org/) for database access.
* [SAM](https://cdcvs.fnal.gov/redmine/projects/sam-main/wiki)
* [Jobsub](https://cdcvs.fnal.gov/redmine/projects/jobsub/wiki)

Client side web pages are built with a combination of

* [html](http://www.w3schools.com/default.asp)
* [javascript](https://www.javascript.com/)
* [jquery](http://jquery.com/)
* [semantic-ui](http://semantic-ui.com/)
* [jinja2](http://jinja.pocoo.org/)

Below is a detailed view of the "Web Services".

![Web Services]({{ site.url }}/docs/images/poms_webservice.png)

POMS web services are established by service.py. service.py configures an instance of Cherrypy, establishes PostgreSQL database communications and invokes the event loop for Cherrypy with poms_service.py as the controller.

poms_serice.py is responsible for managing communications between the business layer and Cherrypy. It also processes Jinja2 templates for presenting business layer data to the end user.

Business Services is composed of a set of discrete classes, each of which supports a specific set of business functions. The main class are shown in the diagram. There is no horizontal communication across these classes. However, communication exists vertically to poms_service and they may call a sub classes as shown in the diagram. All classes in Business Services may call generic utility functions or manage the database through the Database Model.

Below you can find a brief description of the how the frameworks are coded in POMS:

![Frameworks Descriptions]({{ site.url }}/docs/images/POMSelements.png)

