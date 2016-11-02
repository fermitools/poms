#!/usr/bin/env python


### This module contain the methods that handle the Calendar.
### List of methods: calendar_json, calendar, add_event, service_downtimes, update_service, service_status
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel. September, 2016.


# CalendarPOMS.calendar_json(cherrypy.request.db,start, end, timezone)
from model.poms_model import Service, ServiceDowntime, Experimenter
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from utc import utc

class CalendarPOMS:

    def calendar_json(self, dbhandle,start, end, timezone, _):
        rows = dbhandle.query(ServiceDowntime, Service).filter(ServiceDowntime.service_id == Service.service_id).filter(ServiceDowntime.downtime_started.between(start, end)).filter(Service.name != "All").filter(Service.name != "DCache").filter(Service.name != "Enstore").filter(Service.name != "SAM").filter(~Service.name.endswith("sam")).all()
        list=[]
        for row in rows:
            if row.ServiceDowntime.downtime_type == 'scheduled':
                    editable = 'true'
            else:
                editable = 'false'

            if row.Service.name.lower().find("sam") != -1:
                color = "#73ADA2"
            elif row.Service.name.lower().find("fts") != -1:
                color = "#5D8793"
            elif row.Service.name.lower().find("dcache") != -1:
                color = "#1BA8DD"
            elif row.Service.name.lower().find("enstore") != -1:
                color = "#2C7BE0"
            elif row.Service.name.lower().find("fifebatch") != -1:
                color = "#21A8BD"
            else:
                color = "red"
            list.append({'start_key': str(row.ServiceDowntime.downtime_started), 'title': row.Service.name, 's_id': row.ServiceDowntime.service_id, 'start': str(row.ServiceDowntime.downtime_started), 'end': str(row.ServiceDowntime.downtime_ended), 'editable': editable, 'color': color})

        return list


    #CalendarPOMS.calendar(cherrypy.request.db)
    def calendar(self, dbhandle):
        rows = dbhandle.query(Service).filter(Service.name != "All").filter(Service.name != "DCache").filter(Service.name != "Enstore").filter(Service.name != "SAM").filter(Service.name != "FifeBatch").filter(~Service.name.endswith("sam")).all()
        return rows


    #CalendarPOMS.edit_event(cherrypy.request.db, title, start, new_start, end, s_id)
    def add_event(self, dbhandle,title, start, end):

        start_dt = datetime.fromtimestamp(float(start), tz=utc)
        end_dt = datetime.fromtimestamp(float(end), tz=utc)
        s = dbhandle.query(Service).filter(Service.name == title).first()

        if s:
            try:
            #we got a service id
                d = ServiceDowntime()
                d.service_id = s.service_id
                d.downtime_started = start_dt
                d.downtime_ended = end_dt
                d.downtime_type = 'scheduled'
                dbhandle.add(d)
                dbhandle.commit()
                return "Ok."
            except exc.IntegrityError:
                return "This item already exists."

        else:
            #no service id
            return "Oops."

    ##################
    #Services related

    #CalendarPOMS.service_downtimes(cherrypy.request.db)
    def service_downtimes(self, dbhandle):
        rows = dbhandle.query(ServiceDowntime, Service).filter(ServiceDowntime.service_id == Service.service_id).all()
        return rows


    #CalendarPOMS.update_service(cherrypy.request.db, dbhandle, log_handle, name, parent, status, host_site, total, failed, description)
    def update_service(self, dbhandle, log_handle, name, parent, status, host_site, total, failed, description):
        s = dbhandle.query(Service).filter(Service.name == name).first()
        if parent:
            p = dbhandle.query(Service).filter(Service.name == parent).first()
            log_handle("got parent %s -> %s" % (parent, p))
            if not p:
                p = Service()
                p.name = parent
                p.status = "unknown"
                p.host_site = "unknown"
                p.updated = datetime.now(utc)
                dbhandle.add(p)
        else:
            p = None

        if not s:
            s = Service()
            s.name = name
            s.parent_service_obj = p
            s.updated =  datetime.now(utc)
            s.host_site = host_site
            s.status = "unknown"
            dbhandle.add(s)
            s = dbhandle.query(Service).filter(Service.name == name).first()

        if s.status != status and status == "bad" and s.service_id:
            # start downtime, if we aren't in one
            d = dbhandle.query(ServiceDowntime).filter(ServiceDowntime.service_id == s.service_id ).order_by(desc(ServiceDowntime.downtime_started)).first()
            if (d == None or d.downtime_ended != None):
                d = ServiceDowntime()
                d.service_id = s.service_id
                d.downtime_started = datetime.now(utc)
                d.downtime_ended = None
                d.downtime_type = 'actual'
                dbhandle.add(d)

        if s.status != status and status == "good":
            # end downtime, if we're in one
            d = dbhandle.query(ServiceDowntime).filter(ServiceDowntime.service_id == s.service_id ).order_by(desc(ServiceDowntime.downtime_started)).first()
            if d:
                if d.downtime_ended == None:
                    d.downtime_ended = datetime.now(utc)
                    dbhandle.add(d)

        s.parent_service_obj = p
        s.status = status
        s.host_site = host_site
        s.updated = datetime.now(utc)
        s.description = description
        s.items = total
        s.failed_items = failed
        dbhandle.add(s)
        dbhandle.commit()

        return "Ok."


    def service_status(self, dbhandle, under = 'All'):
        prev = None
        prevparent = None
        p = dbhandle.query(Service).filter(Service.name == under).first()
        list = []
        for s in dbhandle.query(Service).filter(Service.parent_service_id == p.service_id).all():
            if s.host_site:
                 url = s.host_site
            else:
                 url = "./service_status?under=%s" % s.name
            list.append({'name': s.name,'status': s.status, 'url': url})

        return list
