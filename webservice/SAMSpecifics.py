from . import logit
from .poms_model import CampaignDependency


class sam_specifics:
    """ 
        All code that needs to change if we replace SAM
        ... except for samweb_lite.py
    """

    def __init__(self, ctx):
        self.ctx = ctx

    def list_files(self, dims):
        return self.ctx.sam.list_files(self.ctx.experiment, dims, dbhandle=self.ctx.db)

    def update_project_description(self, projname, s):
        return self.ctx.sam.update_project_description(self.ctx.experiment, projname, s)

    def get_dataset_from_project(self, submission):
        details = self.ctx.sam.fetch_info(submission.campaign_stage_snapshot_obj.experiment, submission.project, self.ctx.db)
        logit.log("got details = %s" % repr(details))
        dataset = details.get("dataset_def_name", None)
        return dataset

    def create_recovery_dataset(self, s, rtype, rlist):

        isparentof = "isparentof: " * s.campaign_stage_obj.output_ancestor_depth
        isclose = ")" * s.campaign_stage_obj.output_ancestor_depth

        param_overrides = rlist[s.recovery_position].param_overrides
        if rtype.name == "consumed_status":
            # not using ctx.sam.recovery_dimensions here becuase it
            # doesn't do what I want on ended incomplete projects, etc.
            # so not a good choice for our default option.
            recovery_dims = "project_name %s minus (project_name %s and consumed_status consumed)" % (s.project, s.project)
        elif rtype.name == "proj_status":
            recovery_dims = self.ctx.sam.recovery_dimensions(
                s.job_type_snapshot_obj.experiment, s.project, useprocess=1, dbhandle=self.ctx.db
            )

            # recovery dimensions can return an empty string if there is nothing to do.
            if not recovery_dims:
                return 0, ""
        elif rtype.name == "added_files":
            if s.submission_params and s.submission_params.get("dataset"):
                dataset = s.submission_params.get("dataset")
            elif s.project:
                # details = samhandle.fetch_info(
                details = ctx.sam.fetch_info(experiment, s.project, dbhandle)
                dataset = details["dataset"]
            else:
                dataset = None

            recovery_dims = "defname:%s minus snapshot_for_project_name %s" % (dataset, s.project)
        elif rtype.name == "delivered_not_consumed":
            recovery_dims = "project_name %s and consumed_status skipped,unknown,delivered,transferred,unconsumed,cancelled"
        elif rtype.name == "pending_files":
            recovery_dims = "snapshot_for_project_name %s minus ( " % s.project
            if s.job_type_snapshot_obj.output_file_patterns:
                oftypelist = s.job_type_snapshot_obj.output_file_patterns.split(",")
            else:
                oftypelist = ["%"]

            sep = ""
            cdate = s.created.strftime("%Y-%m-%dT%H:%M:%S%z")
            for oft in oftypelist:
                if oft.find(" ") > 0:
                    # it is a dimension not a file_name pattern
                    dim_bits = oft
                else:
                    dim_bits = "file_name like %s" % oft

                recovery_dims += "%s %s version %s and %s and create_date > '%s' %s " % (
                    sep,
                    isparentof,
                    s.campaign_stage_snapshot_obj.software_version,
                    dim_bits,
                    cdate,
                    isclose,
                )
                sep = "and"
            recovery_dims += ")"
        else:
            # default to consumed status(?)
            recovery_dims = "project_name %s minus (project_name %s and consumed_status consumed)" % (s.project, s.project)

        try:
            logit.log("counting files dims %s" % recovery_dims)
            nfiles = self.ctx.sam.count_files(s.campaign_stage_snapshot_obj.experiment, recovery_dims, dbhandle=self.ctx.db)
        except BaseException as be:
            logit.log("exception %s counting files" % be)
            # if we can's count it, just assume there may be a few for
            # now...
            nfiles = 1

        s.recovery_position = s.recovery_position + 1
        self.ctx.db.add(s)
        self.ctx.db.commit()

        logit.log("recovery files count %d" % nfiles)
        if nfiles > 0:
            rname = "poms_recover_%d_%d" % (s.submission_id, s.recovery_position)

            logit.log(
                "launch_recovery_if_needed: creating dataset for exp=%s name=%s dims=%s"
                % (s.campaign_stage_snapshot_obj.experiment, rname, recovery_dims)
            )

            self.ctx.sam.create_definition(s.campaign_stage_snapshot_obj.experiment, rname, recovery_dims)
        else:
            rname = None

        return nfiles, rname

    def dependency_definition(self, s, jobtype, i):

        # definitions for analysis users have to have the username in them
        # so they can define them in the job, we have to follow the same
        # rule here...
        if s.campaign_stage_obj.creator_role == "analysis":
            dname = "poms_%s_depends_%d_%d" % (s.campaign_stage_obj.experimenter_creator_obj.username, s.submission_id, i)
        else:
            dname = "poms_depends_%d_%d" % (s.submission_id, i)

        isparentof = "isparentof:( " * s.campaign_stage_obj.output_ancestor_depth
        ischildof = "ischildof:( " * s.campaign_stage_obj.output_ancestor_depth
        isclose = ")" * s.campaign_stage_obj.output_ancestor_depth

        if s.campaign_stage_obj.campaign_stage_type == "generator":
            # if we're a generator, the previous stage should have declared it
            return dname

        if jobtype.file_patterns.find(" ") > 0:
            # it is a dimension fragment, not just a file pattern
            dim_bits = jobtype.file_patterns
        else:
            dim_bits = "file_name like '%s'" % jobtype.file_patterns
        cdate = s.created.strftime("%Y-%m-%dT%H:%M:%S%z")
        ndate = s.updated.strftime("%Y-%m-%dT%H:%M:%S%z")

        if s.campaign_stage_obj.campaign_stage_type in ("approval", "datatransfer"):
            basedims = "defname:%s" % s.submission_params.get("dataset", dname)
        else:
            basedims = "%s snapshot_for_project_name %s %s and version %s and create_date > '%s'" % (
                ischildof,
                s.project,
                isclose,
                s.campaign_stage_snapshot_obj.software_version,
                cdate,
            )
            # if we have an updated time past our creation, use it for the
            # time window -- this makes our dependency definition basically
            # frozen after we run, so it doesn't collect later similar projects
            # output.
            if ndate != cdate:
                basedims = "%s and create_date <= '%s'" % (basedims, ndate)

        dims = "%s and %s" % (basedims, dim_bits)

        try:
            self.ctx.sam.create_definition(s.campaign_stage_snapshot_obj.experiment, dname, dims)
        except:
            logit.log("ignoring definition error")
        return dname

    def get_file_stats_for_submissions(self, submission_list, experiment, just_output=False):
        #
        # fetch needed data in tandem
        # -- first build lists of stuff to fetch
        #
        base_dim_list = []
        summary_needed = []
        some_kids_needed = []
        some_kids_decl_needed = []
        all_kids_needed = []
        all_kids_decl_needed = []
        output_files = []
        # finished_flying_needed = []
        for s in submission_list:
            summary_needed.append(s)
            basedims = "snapshot_for_project_name %s " % s.project
            base_dim_list.append(basedims)

            isparentof = "isparentof:( " * s.campaign_stage_obj.output_ancestor_depth
            ischildof = "ischildof:(" * s.campaign_stage_obj.output_ancestor_depth
            isclose = ")" * s.campaign_stage_obj.output_ancestor_depth

            somekiddims = "%s and %s version %s %s" % (
                basedims,
                isparentof,
                s.campaign_stage_snapshot_obj.software_version,
                isclose,
            )
            some_kids_needed.append(somekiddims)

            somekidsdecldims = "%s and %s version %s with availability anylocation %s" % (
                basedims,
                isparentof,
                s.campaign_stage_snapshot_obj.software_version,
                isclose,
            )
            some_kids_decl_needed.append(somekidsdecldims)

            allkiddecldims = basedims
            allkiddims = basedims
            for pat in str(s.job_type_snapshot_obj.output_file_patterns).split(","):
                if pat == "None":
                    pat = "%"
                if pat.find(" ") > 0:
                    dimbits = pat
                else:
                    dimbits = "file_name like '%s'" % pat

                allkiddims = "%s and %s %s and version '%s' %s" % (
                    allkiddims,
                    isparentof,
                    dimbits,
                    s.campaign_stage_snapshot_obj.software_version,
                    isclose,
                )
                cdate = s.created.strftime("%Y-%m-%dT%H:%M:%S%z")
                allkiddecldims = (
                    "%s and %s "
                    "%s and version '%s' "
                    "and create_date > '%s' "
                    "with availability anylocation %s "
                    % (allkiddecldims, isparentof, dimbits, s.campaign_stage_snapshot_obj.software_version, cdate, isclose)
                )
                outputfiledims = "%s  %s %s and create_date > '%s' and  %s and version '%s'" % (
                    ischildof,
                    basedims,
                    isclose,
                    s.created.strftime("%Y-%m-%d %H:%M:%S"),
                    dimbits,
                    s.campaign_stage_snapshot_obj.software_version,
                )
            all_kids_needed.append(allkiddims)
            all_kids_decl_needed.append(allkiddecldims)
            output_files.append(outputfiledims)
        #
        # -- now call parallel fetches for items
        #
        output_list = self.ctx.sam.count_files_list(experiment, output_files)
        if just_output:
            summary_list = None
            some_kids_list = None
            some_kids_decl_list = None
            all_kids_decl_list = None
        else:
            summary_list = self.ctx.sam.fetch_info_list(summary_needed, dbhandle=self.ctx.db)
            some_kids_list = self.ctx.sam.count_files_list(experiment, some_kids_needed)
            some_kids_decl_list = self.ctx.sam.count_files_list(experiment, some_kids_decl_needed)
            all_kids_decl_list = self.ctx.sam.count_files_list(experiment, all_kids_decl_needed)
        return (
            summary_list,
            some_kids_decl_needed,
            some_kids_needed,
            base_dim_list,
            output_files,
            output_list,
            all_kids_decl_needed,
            some_kids_list,
            some_kids_decl_list,
            all_kids_decl_list,
        )


class sam_project_checker:
    def __init__(self, ctx):
        self.n_project = 0
        self.lookup_exp_list = []
        self.lookup_submission_list = []
        self.lookup_dims_list = []
        self.ctx = ctx

    def get_file_patterns(self, s):
        plist = []

        # try to get the file pattern list, either from the
        # dependencies that lead to this campaign_stage,
        # or from the job type

        for dcd in (
            self.ctx.db.query(CampaignDependency)
            .filter(CampaignDependency.needs_campaign_stage_id == s.campaign_stage_snapshot_obj.campaign_stage_id)
            .all()
        ):
            if dcd.file_patterns:
                plist.extend(dcd.file_patterns.split(","))
            else:
                plist.append("%")

        if not plist:
            plist = str(s.job_type_snapshot_obj.output_file_patterns).split(",")

        logit.log("got file pattern list: %s" % repr(plist))
        return plist

    def add_project_submission(self, submission):

        self.n_project = self.n_project + 1

        isparentof = "isparentof:(" * submission.campaign_stage_obj.output_ancestor_depth
        isclose = ")" * submission.campaign_stage_obj.output_ancestor_depth

        basedims = "snapshot_for_project_name %s " % submission.project
        allkiddims = basedims
        plist = self.get_file_patterns(submission)

        for pat in plist:
            if pat == "None":
                pat = "%"

            if pat.find(" ") < 0:
                pat = "file_name %s" % pat

            allkiddims = "%s and %s %s and version '%s' and create_date > '%s'  with availability physical %s " % (
                allkiddims,
                isparentof,
                pat,
                submission.campaign_stage_snapshot_obj.software_version,
                submission.created.strftime("%Y-%m-%dT%H:%M:%S%z"),
                isclose,
            )

        self.lookup_exp_list.append(submission.campaign_stage_snapshot_obj.experiment)
        self.lookup_submission_list.append(submission)
        self.lookup_dims_list.append(allkiddims)

    def add_non_project_submission(self, submission):
        # it's located but there's no project, so assume they are
        # defining the poms_depends_%(submission_id)s_1 dataset..
        if submission.campaign_stage_obj.creator_role == "analysis":
            allkiddims = "defname:poms_%s_depends_%s_1" % (
                submission.campaign_stage_obj.experimenter_creator_obj.username,
                submission.submission_id,
            )
        else:
            allkiddims = "defname:poms_depends_%s_1" % submission.submission_id
        self.lookup_exp_list.append(submission.campaign_stage_snapshot_obj.experiment)
        self.lookup_submission_list.append(submission)
        self.lookup_dims_list.append(allkiddims)

    def check_added_submissions(self, finish_up_submissions, res):

        summary_list = self.ctx.sam.fetch_info_list(self.lookup_submission_list, dbhandle=self.ctx.db)
        count_list = self.ctx.sam.count_files_list(self.lookup_exp_list, self.lookup_dims_list)
        thresholds = []
        logit.log("wrapup_tasks: summary_list: %s" % repr(summary_list))  # Check if that is working
        res.append("wrapup_tasks: summary_list: %s" % repr(summary_list))

        res.append("count_list: %s" % count_list)
        res.append("thresholds: %s" % thresholds)
        res.append("lookup_dims_list: %s" % self.lookup_dims_list)

        for i in range(len(summary_list)):
            submission = self.lookup_submission_list[i]
            cfrac = submission.campaign_stage_snapshot_obj.completion_pct / 100.0
            if submission.project:
                threshold = summary_list[i].get("tot_consumed", 0) * cfrac
            else:
                # no project, so guess based on number of jobs in submit
                # command?
                p1 = submission.command_executed.find("-N")
                p2 = submission.command_executed.find(" ", p1 + 3)
                try:
                    threshold = int(submission.command_executed[p1 + 3 : p2]) * cfrac
                except BaseException:
                    threshold = 0

            thresholds.append(threshold)
            val = float(count_list[i])
            res.append("submission %s val %f threshold %f " % (submission, val, threshold))
            if val >= threshold and threshold > 0:
                res.append("adding submission %s " % submission)
                finish_up_submissions.append(submission.submission_id)

        return finish_up_submissions, res