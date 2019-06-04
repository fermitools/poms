
class sam_project_checker:
    def __init__(self, ctx):
        self.n_project = 0
        self.lookup_exp_list = []
        self.lookup_submission_list = []
        self.lookup_dims_list = []
        self.ctx = ctx

    def get_file_patterns(self,s):
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
    
        self.n_project = n_project + 1

        basedims = "snapshot_for_project_name %s " % s.project
        allkiddims = basedims
        plist = self.get_file_patterns(s)

        for pat in plist:
            if pat == "None":
                pat = "%"

            if pat.find(" ") > 0:
                allkiddims = (
                    "%s and isparentof: ( %s and version '%s' and create_date > '%s'  with availability physical ) "
                    % (
                        allkiddims,
                        pat,
                        s.campaign_stage_snapshot_obj.software_version,
                        s.created.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    )
                )
            else:
                allkiddims = (
                    "%s and isparentof: ( file_name '%s' and version '%s' and create_date > '%s' with availability physical ) "
                    % (
                        allkiddims,
                        pat,
                        s.campaign_stage_snapshot_obj.software_version,
                        s.created.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    )
                )

        self.lookup_exp_list.append(s.campaign_stage_snapshot_obj.experiment)
        self.lookup_submission_list.append(s)
        self.lookup_dims_list.append(allkiddims)

    def add_non_project_submission(self, submission):
        # it's located but there's no project, so assume they are
        # defining the poms_depends_%(submission_id)s_1 dataset..
        allkiddims = "defname:poms_depends_%s_1" % s.submission_id
        self.lookup_exp_list.append(s.campaign_stage_snapshot_obj.experiment)
        self.lookup_submission_list.append(s)
        self.lookup_dims_list.append(allkiddims)

    def check_added_submissions(self, finish_up_submissions):

        summary_list = self.ctx.sam.fetch_info_list(lookup_submission_list, dbhandle=ctx.db)
        count_list = self.ctx.sam.count_files_list(lookup_exp_list, lookup_dims_list)
        thresholds = deque()
        logit.log("wrapup_tasks: summary_list: %s" % repr(summary_list))  # Check if that is working
        res.append("wrapup_tasks: summary_list: %s" % repr(summary_list))

        res.append("count_list: %s" % count_list)
        res.append("thresholds: %s" % thresholds)
        res.append("lookup_dims_list: %s" % lookup_dims_list)

        for i in range(len(summary_list)):
            submission = lookup_submission_list[i]
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


        return finish_up_submissions
