#!/usr/bin/env python

### This module contain the methods that handle the
### List of methods: list_generic, edit_screen_generic, update_generic, update_for, edit_screen_for,
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
### written by Marc Mengel, Stephen White and Michael Gueith.
### November, 2016.

import logit
from datetime import datetime
from sqlalchemy import Integer, DateTime, ForeignKey, text

from utc import utc


class TablesPOMS(object):

    def __init__(self, ps):
        self.poms_service = ps
        self.make_admin_map()

    def list_generic(self, dbhandle, err_res, gethead, seshandle, classname):
        if not seshandle.get('experimenter').is_root():
            raise err_res(401, 'You are not authorized to access this resource')
        l = self.make_list_for(dbhandle, self.admin_map[classname], self.pk_map[classname])
        return l

    def edit_screen_generic(self, err_res, gethead, seshandle, classname, id=None):
        if not seshandle.get('experimenter').is_root():
            raise err_res(401, 'You are not authorized to access this resource')
        return self.poms_service.edit_screen_for(classname, self.admin_map[classname], 'update_generic', self.pk_map[classname], id, {})


    def update_generic(self, dbhandle, gethead, seshandle, classname, *args, **kwargs):
        if not seshandle.get('experimenter').is_root():
            return "Not allowed"
        return self.update_for(dbhandle, classname, self.admin_map[classname], self.pk_map[classname], *args, **kwargs)

    def update_for(self, dbhandle, classname, eclass, primkey, *args, **kwargs):   # this method was deleded from the main script
        found = None
        kval = None
        if kwargs.get(primkey, '') != '':
            kval = kwargs.get(primkey, None)
            try:
                kval = int(kval)
                pred = "%s = %d" % (primkey, kval)
            except:
                pred = "%s = '%s'" % (primkey, kval)
            found = dbhandle.query(eclass).filter(text(pred)).first()
            logit.log("update_for: found existing %s" % found)
        if found is None:
            logit.log("update_for: making new %s" % eclass)
            found = eclass()  ####??? Where is this come from?
            if hasattr(found, 'created'):
                setattr(found, 'created', datetime.now(utc))
        columns = found._sa_instance_state.class_.__table__.columns
        for fieldname in columns.keys():
            if not kwargs.get(fieldname, None):
                continue
            if columns[fieldname].type == Integer:
                setattr(found, fieldname, int(kwargs.get(fieldname, '')))
            elif columns[fieldname].type == DateTime:
                # special case created, updated fields; set created
                # if its null, and always set updated if we're updating
                if fieldname == "created" and getattr(found, fieldname, None) is None:
                    setattr(found, fieldname, datetime.now(utc))
                if fieldname == "updated" and kwargs.get(fieldname, None) is None:
                    setattr(found, fieldname, datetime.now(utc))
                if kwargs.get(fieldname, None) is not None:
                    setattr(found, fieldname, datetime.strptime(kwargs.get(fieldname, '')).replace(tzinfo=utc), "%Y-%m-%dT%H:%M")

            elif columns[fieldname].type == ForeignKey:
                kval = kwargs.get(fieldname, None)
                try:
                    kval = int(kval)
                except:
                    pass
                setattr(found, fieldname, kval)
            else:
                setattr(found, fieldname, kwargs.get(fieldname, None))
        logit.log("update_for: found is now %s" % found)
        dbhandle.add(found)
        dbhandle.commit()
        if classname == "Task":
            self.poms_service.snapshot_parts(found. campaign_id)
        return "%s=%s" % (classname, getattr(found, primkey))


    def edit_screen_for(self, dbhandle, gethead, seshandle, classname, eclass, update_call, primkey, primval, valmap):
        if not seshandle.get('experimenter').is_root():
            raise err_res(401, 'You are not authorized to access this resource')

        found = None
        sample = eclass()
        if primval != '':
            logit.log("looking for %s in %s" % (primval, eclass))
            try:
                primval = int(primval)
                pred = "%s = %d" % (primkey, primval)
            except:
                pred = "%s = '%s'" % (primkey, primval)
                pass
            found = dbhandle.query(eclass).filter(text(pred)).first()
            logit.log("found %s" % found)
        if not found:
            found = sample
        columns = sample._sa_instance_state.class_.__table__.columns
        fieldnames = columns.keys()
        screendata = []
        for fn in fieldnames:
            screendata.append({
                'name': fn,
                'primary': columns[fn].primary_key,
                'value': getattr(found, fn, ''),
                'values': valmap.get(fn, None)
            })
        return screendata


    def make_list_for(self, dbhandle, eclass, primkey):     # this function was eliminated from the main class.
        res = []
        for i in dbhandle.query(eclass).order_by(primkey).all():
            res.append( {"key": getattr(i,primkey,''), "value": getattr(i,'name',getattr(i,'username','unknown'))})
        return res


    def make_admin_map(self):   # This method was deleted from the main script.
        """
            make self.admin_map a map of strings to model class names
            and self.pk_map a map of primary keys for that class
        """
        logit.log(" ---- make_admin_map: starting...")
        import poms.model.poms_model
        self.admin_map = {}
        self.pk_map = {}
        for k in poms.model.poms_model.__dict__.keys():
            if hasattr(poms.model.poms_model.__dict__[k], '__module__') and poms.model.poms_model.__dict__[k].__module__ == 'poms.model.poms_model':
                self.admin_map[k] = poms.model.poms_model.__dict__[k]
                found = self.admin_map[k]()
                columns = found._sa_instance_state.class_.__table__.columns
                for fieldname in columns.keys():
                    if columns[fieldname].primary_key:
                        self.pk_map[k] = fieldname
        logit.log(" ---- admin map: %s " % repr(self.admin_map))
        logit.log(" ---- pk_map: %s " % repr(self.pk_map))
