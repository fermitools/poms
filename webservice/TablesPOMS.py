#!/usr/bin/env python

### This module contain the methods that handle the
### List of methods: list_generic, edit_screen_generic, update_generic, update_for, edit_screen_for,
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
### November, 2016.


from datetime import datetime, tzinfo,timedelta


class TablesPOMS:

    def __init__(self, ps, loghandle):
        self.poms_service=ps
        self.make_admin_map(loghandle)
        #dbhandle = dbhandle
        #gethead = gethead
        #loghandle = loghandle


    def list_generic(self, dbhandle, err_res, gethead, seshandle, classname):
        if not self.poms_service.accessPOMS.can_db_admin(gethead, seshandle):
            raise err_res(401, 'You are not authorized to access this resource')
        l = self.make_list_for(dbhandle, self.poms_service.admin_map[classname],self.poms_service.pk_map[classname])
        return l

    def edit_screen_generic(self, err_res, gethead, seshandle, classname, id = None):
        if not self.poms_service.accessPOMS.can_db_admin(gethead, seshandle):
            raise err_res(401, 'You are not authorized to access this resource')
        # XXX -- needs to get select lists for foreign key fields...
        return self.poms_service.edit_screen_for(classname, self.poms_service.admin_map[classname], 'update_generic', self.poms_service.pk_map[classname], id, {})


    def update_generic( self, dbhandle, gethead, loghandle, seshandle, classname, *args, **kwargs):
        if not self.poms_service.accessPOMS.can_report_data( gethead, loghandle, seshandle ):
            return "Not allowed"
        return self.update_for(dbhandle, loghandle, classname, self.poms_service.admin_map[classname], self.poms_service.pk_map[classname], *args, **kwargs)


    def update_for( self, dbhandle, loghandle, classname, eclass, primkey,  *args , **kwargs): #this method was deleded from the main script
        found = None
        kval = None
        if kwargs.get(primkey,'') != '':
            kval = kwargs.get(primkey,None)
            try:
                kval = int(kval)
                pred = "%s = %d" % (primkey, kval)
            except:
                pred = "%s = '%s'" % (primkey, kval)
            found = dbhandle.query(eclass).filter(text(pred)).first()
            loghandle("update_for: found existing %s" % found )
        if found == None:
            loghandle("update_for: making new %s" % eclass)
            found = eclass()  ####??? Where is this come from?
            if hasattr(found,'created'):
                setattr(found, 'created', datetime.now(utc))
        columns = found._sa_instance_state.class_.__table__.columns
        for fieldname in columns.keys():
            if not kwargs.get(fieldname,None):
                continue
            if columns[fieldname].type == Integer:
                setattr(found, fieldname, int(kwargs.get(fieldname,'')))
            elif columns[fieldname].type == DateTime:
                # special case created, updated fields; set created
                # if its null, and always set updated if we're updating
                if fieldname == "created" and getattr(found,fieldname,None) == None:
                    setattr(found, fieldname, datetime.now(utc))
                if fieldname == "updated" and kwargs.get(fieldname,None) == None:
                    setattr(found, fieldname, datetime.now(utc))
                if  kwargs.get(fieldname,None) != None:
                    setattr(found, fieldname, datetime.strptime(kwargs.get(fieldname,'')).replace(tzinfo = utc), "%Y-%m-%dT%H:%M")

            elif columns[fieldname].type == ForeignKey:
                kval = kwargs.get(fieldname,None)
                try:
                    kval = int(kval)
                except:
                    pass
                setattr(found, fieldname, kval)
            else:
                setattr(found, fieldname, kwargs.get(fieldname,None))
        loghandle("update_for: found is now %s" % found )
        dbhandle.add(found)
        dbhandle.commit()
        if classname == "Task":
            self.poms_service.snapshot_parts(found.campaign_id)
        return "%s=%s" % (classname, getattr(found,primkey))


    def edit_screen_for( self, dbhandle, loghandle, gethead, seshandle, classname, eclass, update_call, primkey, primval, valmap):
        if not self.poms_service.accessPOMS.can_db_admin(gethead, seshandle):
            raise err_res(401, 'You are not authorized to access this resource')

        found = None
        sample = eclass()
        if primval != '':
            loghandle("looking for %s in %s" % (primval, eclass))
            try:
                primval = int(primval)
                pred = "%s = %d" % (primkey,primval)
            except:
                pred = "%s = '%s'" % (primkey,primval)
                pass
            found = dbhandle.query(eclass).filter(text(pred)).first()
            loghandle("found %s" % found)
        if not found:
            found = sample
        columns =  sample._sa_instance_state.class_.__table__.columns
        fieldnames = columns.keys()
        screendata = []
        for fn in fieldnames:
            screendata.append({
                'name': fn,
                'primary': columns[fn].primary_key,
                'value': getattr(found, fn, ''),
                'values' : valmap.get(fn, None)
                })
        return screendata


    def make_list_for(self, dbhandle, eclass,primkey): #this function was eliminated from the main class.
        res = []
        for i in dbhandle.query(eclass).order_by(primkey).all():
            res.append( {"key": getattr(i,primkey,''), "value": getattr(i,'name',getattr(i,'email','unknown'))})
        return res


    def make_admin_map(self,loghandle): #This method was deleted from the main script.
        """
            make self.admin_map a map of strings to model class names
            and self.pk_map a map of primary keys for that class
        """
        loghandle(" ---- make_admin_map: starting...")
        import model.poms_model
        self.poms_service.admin_map = {}
        self.poms_service.pk_map = {}
        for k in model.poms_model.__dict__.keys():
            if hasattr(model.poms_model.__dict__[k],'__module__') and model.poms_model.__dict__[k].__module__ == 'model.poms_model':
                self.poms_service.admin_map[k] = model.poms_model.__dict__[k]
                found = self.poms_service.admin_map[k]()
                columns = found._sa_instance_state.class_.__table__.columns
                for fieldname in columns.keys():
                    if columns[fieldname].primary_key:
                         self.poms_service.pk_map[k] = fieldname
        loghandle(" ---- admin map: %s " % repr(self.poms_service.admin_map))
        loghandle(" ---- pk_map: %s " % repr(self.poms_service.pk_map))
