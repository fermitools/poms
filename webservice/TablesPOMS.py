#!/usr/bin/env python

# This module contain the methods that handle the
# List of methods:
#   list_generic, edit_screen_generic, update_generic, update_for,
#    edit_screen_for,
# Author: Felipe Alba ahandresf@gmail.com, This code is just a modify
#     version of functions in poms_service.py
# written by Marc Mengel, Stephen White and Michael Gueith.
### November, 2016.

from collections import deque
from datetime import datetime
import json
from sqlalchemy import text
from . import logit
from .utc import utc


class TablesPOMS:

    def __init__(self, ps):
        self.poms_service = ps
        self.make_admin_map()

    def list_generic(self, ctx.db, classname):
        l = self.make_list_for(
            ctx.db, self.admin_map[classname], self.pk_map[classname])
        return l

    def edit_screen_generic(self, classname, id=None):
        return self.poms_service.edit_screen_for(
            classname, self.admin_map[classname], 'update_generic', self.pk_map[classname], id, {})

    def update_generic(self, ctx.db, classname, *args, **kwargs):
        return self.update_for(
            ctx.db, classname, self.admin_map[classname], self.pk_map[classname], *args, **kwargs)

    # this method was deleded from the main script
    def update_for(self, ctx.db, classname,
                   eclass, primkey, *args, **kwargs):
        found = None
        kval = None
        if kwargs.get(primkey, '') != '':
            kval = kwargs.get(primkey, None)
            try:
                kval = int(kval)
                pred = "%s = %d" % (primkey, kval)
            except BaseException:
                pred = "%s = '%s'" % (primkey, kval)
            found = ctx.db.query(eclass).filter(text(pred)).first()
            logit.log("update_for: found existing %s" % found)
        if found is None:
            logit.log("update_for: making new %s" % eclass)
            found = eclass()  # ??? Where is this come from?
            if hasattr(found, 'created'):
                setattr(found, 'created', datetime.now(utc))
        columns = found._sa_instance_state.class_.__table__.columns
        for fieldname in list(columns.keys()):
            logit.log("column %s type %s" %
                      (fieldname, columns[fieldname].type))
            if not kwargs.get(fieldname, None):
                continue
            if kwargs.get(fieldname, None) == 'None':
                continue
            if str(columns[fieldname].type) == 'INTEGER':
                setattr(found, fieldname, int(kwargs.get(fieldname, '')))
            if str(columns[fieldname].type) == 'JSON':
                v = json.loads(kwargs.get(fieldname, ''))
                logit.log("json load gives: %s" % repr(v))
                setattr(found, fieldname, v)
            elif str(columns[fieldname].type) == 'BOOLEAN':
                v = kwargs.get(fieldname, 'None')
                logit.log("converting boolean: %s " % v)
                if v == 'False':
                    v = False
                if v == 'True':
                    v = True
                setattr(found, fieldname, v)
            elif str(columns[fieldname].type) == 'DATETIME':
                # special case created, updated fields; set created
                # if its null, and always set updated if we're updating
                if fieldname == "created" and getattr(
                        found, fieldname, None) is None:
                    setattr(found, fieldname, datetime.now(utc))
                if fieldname == "updated" and kwargs.get(
                        fieldname, None) is None:
                    setattr(found, fieldname, datetime.now(utc))
                if kwargs.get(fieldname, None) is not None:
                    setattr(found, fieldname, datetime.strptime(kwargs.get(
                        fieldname, '')[:16], "%Y-%m-%d %H:%M").replace(tzinfo=utc))

            elif str(columns[fieldname].type) == 'ForeignKey':
                kval = kwargs.get(fieldname, None)
                try:
                    kval = int(kval)
                except BaseException:
                    pass
                setattr(found, fieldname, kval)
            else:
                setattr(found, fieldname, kwargs.get(fieldname, None))
        logit.log("update_for: found is now %s" % found)
        ctx.db.add(found)
        ctx.db.commit()
        if classname == "Submission":
            self.poms_service.snapshot_parts(found. campaign_stage_id)
        return "%s=%s" % (classname, getattr(found, primkey))

    def edit_screen_for(self, ctx.db, classname, eclass, update_call, primkey, primval, valmap):

        found = None
        sample = eclass()
        if primval != '':
            logit.log("looking for %s in %s" % (primval, eclass))
            try:
                primval = int(primval)
                pred = "%s = %d" % (primkey, primval)
            except BaseException:
                pred = "%s = '%s'" % (primkey, primval)
            found = ctx.db.query(eclass).filter(text(pred)).first()
            logit.log("found %s" % found)
        if not found:
            found = sample
        columns = found._sa_instance_state.class_.__table__.columns
        fieldnames = list(columns.keys())
        screendata = deque()
        for fn in fieldnames:
            logit.log("found field %s type %s val %s" %
                      (fn, columns[fn].type, getattr(found, fn, '')))
            screendata.append({
                'name': fn,
                'primary': columns[fn].primary_key,
                'value': (
                    json.dumps(getattr(found, fn, '[]'))
                    if str(columns[fn].type) == 'JSON'
                    else str(getattr(found, fn, ''))),
                'values': valmap.get(fn, None)
            })
        return screendata

    # this function was eliminated from the main class.
    def make_list_for(self, ctx.db, eclass, primkey):
        res = deque()
        for i in ctx.db.query(eclass).order_by(primkey).all():
            res.append({"key": getattr(i, primkey, ''), "value": getattr(
                i, 'name', getattr(i, 'ctx.usernamename', 'unknown'))})
        return res

    def make_admin_map(self):   # This method was deleted from the main script.
        """
            make self.admin_map a map of strings to model class names
            and self.pk_map a map of primary keys for that class
        """
        logit.log(" ---- make_admin_map: starting...")
        import poms.webservice.poms_model as poms_model
        self.admin_map = {}
        self.pk_map = {}
        for k in dir(poms_model):
            logit.log("key: %s" % k)
            if hasattr(poms_model.__dict__[k], '__module__') and poms_model.__dict__[
                    k].__module__ == 'poms.webservice.poms_model':
                self.admin_map[k] = poms_model.__dict__[k]
                found = self.admin_map[k]()
                columns = found._sa_instance_state.class_.__table__.columns
                for fieldname in list(columns.keys()):
                    if columns[fieldname].primary_key:
                        self.pk_map[k] = fieldname
        logit.log(" ---- admin map: %s " % repr(self.admin_map))
        logit.log(" ---- pk_map: %s " % repr(self.pk_map))
