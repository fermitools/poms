from datetime import datetime
from . import logit
from .poms_model import CampaignDependency, Submission, Campaign, CampaignStage
from sqlalchemy import func, and_
from sqlalchemy.orm.attributes import flag_modified
import uuid
import re


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

    def declare_approval_transfer_datasets(self, sid):
        # we need to declare our "output" datsets to match our input...
        s = self.ctx.db.query(Submission).filter(Submission.submission_id == sid).first()
        cs = s.campaign_stage_obj
        ndeps = (
            self.ctx.db.query(func.count(CampaignDependency.provides_campaign_stage_id))
            .filter(CampaignDependency.needs_campaign_stage_id == s.campaign_stage_snapshot_obj.campaign_stage_id)
            .group_by(CampaignDependency.provides_campaign_stage_id)
            .first()
        )
        for i in range(1, ndeps[0] + 1):
            if cs.creator_role == "analysis":
                dname = "poms_%s_depends_%d_%d" % (s.campaign_stage_obj.experimenter_creator_obj.username, s.submission_id, i)
            else:
                dname = "poms_depends_%d_%d" % (s.submission_id, i)

            logit.log(
                "declare_approval_transfer_datasets: creating definition %s defname:%s" % (dname, s.submission_params["dataset"])
            )

            self.ctx.sam.create_definition(cs.experiment, dname, "defname:%s" % s.submission_params["dataset"])

    def create_recovery_dataset(self, s, rtype, rlist):

        keywords = s.campaign_stage_obj.campaign_obj.campaign_keywords
        isparentof = "isparentof:( " * s.campaign_stage_obj.output_ancestor_depth
        isclose = ")" * s.campaign_stage_obj.output_ancestor_depth

        param_overrides = rlist[s.recovery_position].param_overrides
        if rtype.name == "consumed_status":
            # not using ctx.sam.recovery_dimensions here becuase it
            # doesn't do what I want on ended incomplete projects, etc.
            # so not a good choice for our default option.
            recovery_dims = "snapshot_for_project_name %s minus (project_name %s and consumed_status co%%)" % (
                s.project,
                s.project,
            )
        elif rtype.name == "process_status":
            recovery_dims = self.ctx.sam.recovery_dimensions(
                s.job_type_snapshot_obj.experiment, s.project, useprocess=1, dbhandle=self.ctx.db
            )
            # work around consumed/completed status chagnes
            recovery_dims = recovery_dims.replace("consumed_status consumed", "consumed_status 'co%'")

            # recovery dimensions can return an empty string if there is nothing to do.
            if not recovery_dims:
                return 0, ""
        elif rtype.name == "added_files":
            if s.submission_params and s.submission_params.get("dataset"):
                dataset = s.submission_params.get("dataset")
            elif s.project:
                # details = samhandle.fetch_info(
                details = self.ctx.sam.fetch_info(s.job_type_snapshot_obj.experiment, s.project, self.ctx.db)
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
            # default to consumed_status(?)
            recovery_dims = "snapshot_for_project_name %s minus (project_name %s and consumed_status co%%)" % (
                s.project,
                s.project,
            )

        try:
            recovery_dims = self.try_format_with_keywords(recovery_dims, s.campaign_stage_snapshot_obj.campaign_obj.campaign_keywords)
            logit.log("counting files dims %s" % recovery_dims)
            nfiles = self.ctx.sam.count_files(s.campaign_stage_snapshot_obj.experiment, recovery_dims, dbhandle=self.ctx.db)
           
        except BaseException as be:
            logit.log("exception %s counting files" % be)
            # if we can's count it, just assume there may be a few for
            # now...
            nfiles = 1

        s.recovery_position = s.recovery_position + 1
        
       

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
        
        recovery = {}
        recovery["name"] = rname
        recovery["timestamp"] = datetime.now().isoformat()
        recovery["count"] = nfiles
        recovery["exp"] = s.campaign_stage_snapshot_obj.experiment
        recovery["dims"] = recovery_dims
        workflow = s.submission_params.get("workflow", {})
        recoveries = workflow.get("recoveries", [])
        recoveries.append(recovery)
        workflow["recoveries"] = recoveries
        s.submission_params["workflow"] = workflow
        s.submission_params = s.submission_params
        flag_modified(s, 'submission_params')
        self.ctx.db.add(s)
        self.ctx.db.commit()
        return nfiles, rname

    def dependency_definition(self, s, jobtype, i):

        # definitions for analysis users have to have the username in them
        # so they can define them in the job, we have to follow the same
        # rule here...
        if s.campaign_stage_obj.creator_role == "analysis":
            dname = "poms_%s_depends_%d_%d" % (s.campaign_stage_obj.experimenter_creator_obj.username,  s.submission_id, i)
        else:
            dname = "poms_depends_%d_%d" % (s.submission_id, i)

        isparentof = "isparentof:( " * s.campaign_stage_obj.output_ancestor_depth
        ischildof = "ischildof:( " * s.campaign_stage_obj.output_ancestor_depth
        isclose = ")" * s.campaign_stage_obj.output_ancestor_depth

        if s.campaign_stage_obj.campaign_stage_type == "generator" or not s.project:
            # if we're a generator, the previous stage should have declared it
            # or eventually it doesn't have a SAM project
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
            # .. nevermind, that actually exclues recovery launch output..
            # if ndate != cdate:
            #    basedims = "%s and create_date <= '%s'" % (basedims, ndate)

        cur_dname_dims = "defname:%s" % dname
        cur_dname_nfiles = self.ctx.sam.count_files(
            s.campaign_stage_snapshot_obj.experiment, cur_dname_dims, dbhandle=self.ctx.db
        )

        dims = "%s and %s" % (basedims, dim_bits)
        dims = self.try_format_with_keywords(dims, s.campaign_stage_obj.campaign_obj.campaign_keywords)
        
        new_dname_nfiles = self.ctx.sam.count_files(s.campaign_stage_snapshot_obj.experiment, dims, dbhandle=self.ctx.db)
        logit.log("count files: defname %s has %d files" % (dname, cur_dname_nfiles))
        logit.log("count files: new dimensions has %d files" % new_dname_nfiles)

        # if #files in the current SAM definition are not less than #files in the updated SAM definition
        # we do not need to update it, so keep the DAM definition with current dimensions
        if cur_dname_nfiles >= new_dname_nfiles:
            logit.log("Do not need to update defname: %s" % dname)
            return dname

        try:
            self.ctx.sam.create_definition(s.campaign_stage_snapshot_obj.experiment, dname, dims)
        except:
            logit.log("ignoring definition error")
        
        dependency = {}
        dependency["name"] = dname
        dependency["timestamp"] = datetime.now().isoformat()
        dependency["current_count"] = cur_dname_nfiles
        dependency["new_count"] = new_dname_nfiles
        dependency["exp"] = s.campaign_stage_snapshot_obj.experiment
        dependency["dims"] = dims
        workflow = s.submission_params.get("workflow", {})
        deps = workflow.get("dependencies", [])
        deps.append(dependency)
        workflow["dependencies"] = deps
        s.submission_params["workflow"] = workflow
        s.submission_params = s.submission_params
        flag_modified(s, 'submission_params')
        self.ctx.db.add(s)
        self.ctx.db.commit()

        return dname
    
    # Adds campaign keywords into applicable strings.
    def try_format_with_keywords(self, query, campaign_keywords=None):
        try:
            if campaign_keywords and bool(re.search(r'%\(\w+\)s', query)):
                query = query % campaign_keywords
                return query % campaign_keywords
            else:
                return query
        except Exception as e:
            logit.log("SAMSpecifics | try_format_with_keywords | Error during formatting: %s" % e)
            return query

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
        
        campaign = self.ctx.db.query(Campaign).join(
                CampaignStage, Campaign.campaign_id == CampaignStage.campaign_id
            ).join(
                Submission, Submission.campaign_stage_id == CampaignStage.campaign_stage_id
            ).filter(
                    Submission.submission_id.in_([sub.submission_id for sub in submission_list])
            ).first()
        
        keywords = campaign.campaign_keywords if campaign else None
        
        
        for s in submission_list:
            
            summary_needed.append(s)
            basedims = "snapshot_for_project_name %s " % s.project
            basedims = self.try_format_with_keywords(basedims, keywords)
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
            somekiddims = self.try_format_with_keywords(somekiddims, keywords)
            some_kids_needed.append(somekiddims)

            somekidsdecldims = "%s and %s version %s with availability anylocation %s" % (
                basedims,
                isparentof,
                s.campaign_stage_snapshot_obj.software_version,
                isclose,
            )
            somekidsdecldims = self.try_format_with_keywords(somekidsdecldims, keywords)
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
                    s.created.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    dimbits,
                    s.campaign_stage_snapshot_obj.software_version,
                )
                
            allkiddims = self.try_format_with_keywords(allkiddims, keywords)
            all_kids_needed.append(allkiddims)
            
            allkiddecldims = self.try_format_with_keywords(allkiddecldims, keywords)
            all_kids_decl_needed.append(allkiddecldims)
            
            outputfiledims = self.try_format_with_keywords(outputfiledims, keywords)
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
        subs = set([x.submission_id for x in submission_list])
        existing = self.ctx.db.query(Submission).filter(Submission.submission_id.in_(subs)).all()
        if existing and summary_list:
            for i in range(0,len(submission_list)):
                logit.log("sub_%d: %s" % (i, submission_list[i]))
                s = [p for p in existing if p.submission_id == submission_list[i].submission_id]
                logit.log("sub_%d, got s: %s" % (i,repr(s)))
                if s:
                    s[0].files_consumed = summary_list[i].get("tot_consumed", 0)
                    s[0].files_generated = summary_list[i].get("files_in_snapshot", 0)
                    self.ctx.db.add(s[0])
                   
        self.ctx.db.commit()
        
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

    def count_files_in_datasets(self, exp, datasets):
        cdl = []
        for d in datasets:
            cdl.append("defname:%s" % d)
        dims = " or ".join([c for c in cdl if c != None])
        logit.log("count_files_in_datasets: Counting dimension %s" % dims)
        return self.ctx.sam.count_files(exp, dims, dbhandle=self.ctx.db)


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

        isparentof = "isparentof:( " * submission.campaign_stage_obj.output_ancestor_depth
        isclose = ")" * submission.campaign_stage_obj.output_ancestor_depth

        basedims = "snapshot_for_project_name %s " % submission.project
        allkiddims = basedims
        plist = self.get_file_patterns(submission)

        prev = None
        for pat in plist:
            if pat == "None":
                pat = "%"

            # don't add duplicated clauses if the dependencies
            # have the same pattern
            if pat == prev:
                continue
            prev = pat

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
            allkiddims = "defname:poms_depends_%s_1" % (submission.submission_id)
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
            if submission.files_consumed == None:
                submission.files_consumed = summary_list[i].get("tot_consumed", 0)
            if submission.files_generated == None:
                submission.files_generated = count_list[i]

            if submission.campaign_stage_snapshot_obj.completion_type == "complete":
                # not zero, but should always pass...
                threshold = 0.00001

            elif submission.project:
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
            if submission.campaign_stage_snapshot_obj.completion_type == "complete":
                if val == -1.0:
                    val = 0.1
            res.append("submission %s val %f threshold %f " % (submission.submission_id, val, threshold))
            if val >= threshold and (threshold != 0 or submission.recovery_tasks_parent):
                res.append("adding submission %s " % submission)
                finish_up_submissions.add(submission.submission_id)

        return finish_up_submissions, res
