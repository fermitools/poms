#!/usr/bin/env python


# This module contain the methods that handle the Calendar. (5 methods)
# List of methods: handle_dates, quick_search, jump_to_job,
# Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
# written by Marc Mengel, Stephen White and Michael Gueith.
### October, 2016.
from datetime import datetime, timedelta
from .utc import utc
from .poms_model import Experimenter


class UtilsPOMS:
    def __init__(self, ps):
        self.poms_service = ps

    # this method was deleted from the main script
    def handle_dates(self, tmin, tmax, tdays, baseurl):
        """
        tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange = self.handle_dates(tmax, tdays, name)
        assuming tmin, tmax, are date strings or None, and tdays is
        an integer width in days, come up with real datetimes for
        tmin, tmax, and string versions, and next ane previous links
        and a string describing the date range.  Use everywhere.
        """

        # if they set max and min (i.e. from calendar) set tdays from that.
        if not tmax in (None, "") and not tmin in (None, ""):
            if isinstance(tmin, str):
                tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
            if isinstance(tmax, str):
                tmax = datetime.strptime(tmax[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
            tdays = (tmax - tmin).total_seconds() / 86400.0

        if tmax in (None, ""):
            if tmin not in (None, "") and tdays not in (None, ""):
                if isinstance(tmin, str):
                    tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
                tmax = tmin + timedelta(days=float(tdays))
            else:
                # if we're not given a max, pick now
                tmax = datetime.now(utc)

        elif isinstance(tmax, str):
            tmax = datetime.strptime(tmax[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)

        if tdays in (None, ""):  # default to one day
            tdays = 1

        tdays = float(tdays)

        if tmin in (None, ""):
            tmin = tmax - timedelta(days=tdays)

        elif isinstance(tmin, str):
            tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)

        tsprev = tmin.strftime("%Y-%m-%d+%H:%M:%S")
        tsnext = (tmax + timedelta(days=tdays)).strftime("%Y-%m-%d+%H:%M:%S")
        tmax_s = tmax.strftime("%Y-%m-%d %H:%M:%S")
        tmin_s = tmin.strftime("%Y-%m-%d %H:%M:%S")
        prevlink = "%s/%stmax=%s&tdays=%d" % (self.poms_service.path, baseurl, tsprev, tdays)
        nextlink = "%s/%stmax=%s&tdays=%d" % (self.poms_service.path, baseurl, tsnext, tdays)
        # if we want to handle hours / weeks nicely, we should do
        # it here.
        plural = "s" if tdays > 1.0 else ""
        trange = '%6.1f day%s ending <span class="tmax">%s</span>' % (tdays, plural, tmax_s)

        # redundant, but trying to rule out tz woes here...
        tmin = tmin.replace(tzinfo=utc)
        tmax = tmax.replace(tzinfo=utc)
        tdays = int((tmax - tmin).total_seconds() / 864.0) / 100

        return (tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange, tdays)

    def quick_search(self, redirect, search_term):
        search_term = search_term.strip()
        search_term = search_term.replace("*", "%")
        raise redirect("%s/search_campaigns?search_term=%s" % (self.poms_service.path, search_term))

    def getSavedExperimentRole(self, ctx.db, ctx.usernamename):
        experiment, role = (
            ctx.db.query(Experimenter.session_experiment, Experimenter.session_role)
            .filter(Experimenter.ctx.usernamename == ctx.usernamename)
            .first()
        )
        return experiment, role

    def update_session_experiment(self, db, ctx.username, experiment):
        fields = {"session_experiment": experiment}
        db.query(Experimenter).filter(Experimenter.ctx.usernamename == ctx.username).update(fields)
        db.commit()

    def update_session_role(self, db, ctx.username, role):

        db.query(Experimenter).filter(Experimenter.ctx.usernamename == ctx.username).update({"session_role": role})

        db.commit()
