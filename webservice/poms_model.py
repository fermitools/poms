# coding: utf-8
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, text

# from sqlalchemy import Table
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.dialects.postgresql import UUID
import uuid

Base = declarative_base()
metadata = Base.metadata


class Tag(Base):
    __tablename__ = "tags"

    tag_id = Column(Integer, primary_key=True, server_default=text("nextval('campaigns_campaign_id_seq'::regclass)"))
    experiment = Column(ForeignKey("experiments.experiment"), nullable=False, index=True)
    tag_name = Column(Text, nullable=False)
    creator = Column(ForeignKey("experimenters.experimenter_id"), nullable=False, index=True)
    creator_role = Column(Text, nullable=False)

    campaigns = relationship("Campaign", secondary="campaigns_tags", lazy="dynamic")


class CampaignsTag(Base):
    __tablename__ = "campaigns_tags"

    tag_id = Column(Integer, ForeignKey("tags.tag_id"), primary_key=True)
    campaign_id = Column(Integer, ForeignKey("campaigns.campaign_id"), primary_key=True)


class Campaign(Base):
    __tablename__ = "campaigns"

    campaign_id = Column(Integer, primary_key=True, server_default=text("nextval('campaigns_campaign_id_seq'::regclass)"))
    experiment = Column(ForeignKey("experiments.experiment"), nullable=False, index=True)
    name = Column(Text, nullable=False)
    data_handling_service=Column(Text, nullable=False, server_default=text("sam"))
    active = Column(Boolean, nullable=False, server_default=text("true"))
    defaults = Column(JSON)
    creator = Column(ForeignKey("experimenters.experimenter_id"), nullable=False, index=True)
    creator_role = Column(Text, nullable=False)
    campaign_type = Column(Text, nullable=True)
    campaign_keywords = Column(JSON)

    tags = relationship(Tag, secondary="campaigns_tags", lazy="dynamic")
    stages = relationship("CampaignStage", back_populates="campaign_obj", lazy="dynamic")
    experimenter_creator_obj = relationship("Experimenter", primaryjoin="Campaign.creator == Experimenter.experimenter_id")


class CampaignStage(Base):
    __tablename__ = "campaign_stages"

    campaign_stage_id = Column(
        Integer, primary_key=True, server_default=text("nextval('campaign_stages_campaign_stage_id_seq'::regclass)")
    )
    experiment = Column(ForeignKey("experiments.experiment"), nullable=False, index=True)
    name = Column(Text, nullable=False)
    job_type_id = Column(
        ForeignKey("job_types.job_type_id"),
        nullable=False,
        index=True,
        server_default=text("nextval('campaigns_campaign_definition_id_seq'::regclass)"),
    )

    creator = Column(ForeignKey("experimenters.experimenter_id"), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    updater = Column(ForeignKey("experimenters.experimenter_id"), index=True)
    updated = Column(DateTime(True))

    vo_role = Column(Text, nullable=False)
    cs_last_split = Column(Integer, nullable=True)
    cs_split_type = Column(Text, nullable=True)
    cs_split_dimensions = Column(Text, nullable=True)
    dataset = Column(Text, nullable=False)
    software_version = Column(Text, nullable=False)
    login_setup_id = Column(ForeignKey("login_setups.login_setup_id"), nullable=False)
    param_overrides = Column(JSON)
    test_param_overrides = Column(JSON)
    test_split_type = Column(Text, nullable=False, server_default=text(""))
    completion_type = Column(Text, nullable=False, server_default=text("located"))
    completion_pct = Column(Integer, nullable=False, server_default="95")
    hold_experimenter_id = Column(ForeignKey("experimenters.experimenter_id"), nullable=True)
    creator_role = Column(Text, nullable=False)
    role_held_with = Column(Text, nullable=True)
    campaign_stage_type = Column(Text, nullable=False)
    merge_overrides = Column(Boolean, nullable=True)
    output_ancestor_depth = Column(Integer, server_default="1")
    default_clear_cronjob = Column(Boolean, server_default=text("true"), nullable=False)
    data_dispatcher_dataset_query = Column(Text, nullable=True)
    data_dispatcher_project_id = Column(Integer, nullable=True)
    
    campaign_id = Column(ForeignKey("campaigns.campaign_id"), nullable=True, index=True)

    experimenter_creator_obj = relationship("Experimenter", primaryjoin="CampaignStage.creator == Experimenter.experimenter_id")
    experimenter_updater_obj = relationship("Experimenter", primaryjoin="CampaignStage.updater == Experimenter.experimenter_id")
    experimenter_holder_obj = relationship(
        "Experimenter", primaryjoin="CampaignStage.hold_experimenter_id == Experimenter.experimenter_id"
    )
    experiment_obj = relationship("Experiment")
    job_type_obj = relationship("JobType")
    login_setup_obj = relationship("LoginSetup")
    campaign_obj = relationship("Campaign", back_populates="stages")

    providers = relationship(
        "CampaignStage",
        secondary="campaign_dependencies",
        primaryjoin="CampaignStage.campaign_stage_id==CampaignDependency.provides_campaign_stage_id",
        secondaryjoin="CampaignStage.campaign_stage_id==CampaignDependency.needs_campaign_stage_id",
        backref="consumers",
    )
    
    
class DataDispatcherProject(Base):
    __tablename__ = "data_dispatcher_projects"
    data_dispatcher_project_idx = Column(Integer, primary_key=True, server_default="")
    project_id = Column(Integer, nullable=False)
    project_name = Column(Text)
    experiment = Column(Text, nullable=False)
    vo_role = Column(Text, nullable=False)
    campaign_id = Column(Integer)
    campaign_stage_id = Column(ForeignKey("campaign_stages.campaign_stage_id"), nullable=True, index=True)
    campaign_stage_snapshot_id = Column(Integer)
    submission_id = Column(Integer)
    job_type_snapshot_id = Column(ForeignKey("job_type_snapshots.job_type_snapshot_id"), nullable=True, index=True)
    split_type = Column(Integer)
    last_split = Column(Integer)
    depends_on_submission = Column(Integer)
    depends_on_project = Column(Integer)
    recovery_type_id = Column(Integer)
    recovery_tasks_parent_submission = Column(Integer)
    recovery_tasks_parent_project = Column(Integer)
    recovery_position = Column(Integer)
    creator = Column(Integer, nullable=False)
    created = Column(DateTime(True), nullable=False, default="now()")
    updater = Column(Integer)
    updated = Column(DateTime(True))
    worker_timeout = Column(Integer)
    idle_timeout = Column(Integer)
    active = Column(Boolean, nullable=False, default=True)
    jobsub_job_id = Column(Text)
    
    campaign_stage_obj = relationship("CampaignStage", foreign_keys=campaign_stage_id)
    job_type_snapshot_obj = relationship("JobTypeSnapshot", foreign_keys=job_type_snapshot_id)
    
class Experimenter(Base):
    __tablename__ = "experimenters"

    experimenter_id = Column(Integer, primary_key=True, server_default="")
    first_name = Column(Text, nullable=False)
    last_name = Column(Text)
    username = Column(Text, nullable=False)
    last_login = Column(DateTime(True), nullable=False, default="now()")
    session_experiment = Column(Text, nullable=False)
    session_role = Column(Text, nullable=False)
    root = Column(Boolean, nullable=False)


class ExperimentsExperimenters(Base):
    __tablename__ = "experiments_experimenters"

    experimenter_id = Column(Integer, ForeignKey("experimenters.experimenter_id"), primary_key=True)
    experiment = Column(Text, ForeignKey("experiments.experiment"), primary_key=True)
    active = Column(Boolean, nullable=False, server_default=text("true"))
    role = Column(Text, nullable=False, server_default=text("analysis"))

    experimenter_obj = relationship(Experimenter, backref="exp_expers")
    experiment_obj = relationship("Experiment", backref="exp_expers")


class Experiment(Base):
    __tablename__ = "experiments"

    experiment = Column(String(10), primary_key=True)
    name = Column(Text, nullable=False)
    logbook = Column(Text, nullable=True)
    snow_url = Column(Text, nullable=True)
    restricted = Column(Boolean, nullable=False, server_default=text("false"))
    active = Column(Boolean, nullable=False, server_default=text("true"))

class ExperimentersWatching(Base):
    __tablename__ = "experimenters_watching"

    experimenters_watching_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    experimenter_id = Column(ForeignKey("experimenters.experimenter_id"),nullable=False, index=True)
    campaign_id = Column(ForeignKey("campaigns.campaign_id"), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    experimenter_obj = relationship(Experimenter, backref="exp_watch")
    campaign_obj = relationship(Campaign, backref="exp_watch")


class LoginSetup(Base):
    __tablename__ = "login_setups"

    login_setup_id = Column(
        Integer, primary_key=True, server_default=text("nextval('login_setups_login_setup_id_seq'::regclass)")
    )
    name = Column(Text, nullable=False, index=True, unique=True)
    experiment = Column(ForeignKey("experiments.experiment"), nullable=False, index=True)
    launch_host = Column(Text, nullable=False)
    launch_account = Column(Text, nullable=False)
    launch_setup = Column(Text, nullable=False)
    creator = Column(ForeignKey("experimenters.experimenter_id"), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    updater = Column(ForeignKey("experimenters.experimenter_id"), index=True)
    updated = Column(DateTime(True))
    creator_role = Column(Text, nullable=False)
    active = Column(Boolean)

    experiment_obj = relationship("Experiment")
    experimenter_creator_obj = relationship("Experimenter", primaryjoin="LoginSetup.creator == Experimenter.experimenter_id")
    experimenter_updater_obj = relationship("Experimenter", primaryjoin="LoginSetup.updater == Experimenter.experimenter_id")


class JobType(Base):
    __tablename__ = "job_types"

    job_type_id = Column(Integer, primary_key=True, server_default=text("nextval('job_types_job_type_id_seq'::regclass)"))
    name = Column(Text, nullable=False, unique=True)
    experiment = Column(ForeignKey("experiments.experiment"), nullable=False, index=True)
    launch_script = Column(Text)
    definition_parameters = Column(JSON)
    input_files_per_job = Column(Integer)
    output_files_per_job = Column(Integer)
    output_file_patterns = Column(Text)
    creator = Column(ForeignKey("experimenters.experimenter_id"), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    updater = Column(ForeignKey("experimenters.experimenter_id"), index=True)
    updated = Column(DateTime(True))
    creator_role = Column(Text, nullable=False)
    active = Column(Boolean)

    experiment_obj = relationship("Experiment")
    experimenter_creator_obj = relationship("Experimenter", primaryjoin="JobType.creator == Experimenter.experimenter_id")
    experimenter_updater_obj = relationship("Experimenter", primaryjoin="JobType.updater == Experimenter.experimenter_id")


class Submission(Base):
    __tablename__ = "submissions"

    submission_id = Column(Integer, primary_key=True, server_default=text("nextval('submissions_submissions_id_seq'::regclass)"))
    campaign_stage_id = Column(
        ForeignKey("campaign_stages.campaign_stage_id"),
        nullable=False,
        index=True,
        server_default=text("nextval('tasks_campaign_id_seq'::regclass)"),
    )
    creator = Column(ForeignKey("experimenters.experimenter_id"), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    submission_params = Column(MutableDict.as_mutable(JSON))
    depends_on = Column(ForeignKey("submissions.submission_id"), index=True)
    depend_threshold = Column(Integer)
    updater = Column(ForeignKey("experimenters.experimenter_id"), index=True)
    updated = Column(DateTime(True))
    command_executed = Column(Text)
    project = Column(Text)
    files_consumed = Column(Integer, nullable=True)
    files_generated = Column(Integer, nullable=True)
    login_setup_snapshot_id = Column(ForeignKey("login_setup_snapshots.login_setup_snapshot_id"), nullable=True, index=True)
    campaign_stage_snapshot_id = Column(
        ForeignKey("campaign_stage_snapshots.campaign_stage_snapshot_id"), nullable=True, index=True
    )
    job_type_snapshot_id = Column(ForeignKey("job_type_snapshots.job_type_snapshot_id"), nullable=True, index=True)
    recovery_position = Column(Integer)
    recovery_tasks_parent = Column(ForeignKey("submissions.submission_id"), index=True)
    jobsub_job_id = Column(Text)

    campaign_stage_obj = relationship("CampaignStage")
    experimenter_creator_obj = relationship("Experimenter", primaryjoin="Submission.creator == Experimenter.experimenter_id")
    experimenter_updater_obj = relationship("Experimenter", primaryjoin="Submission.updater == Experimenter.experimenter_id")
    parent_obj = relationship("Submission", remote_side=[submission_id], foreign_keys=recovery_tasks_parent)
    login_setup_snapshot_obj = relationship("LoginSetupSnapshot", foreign_keys=login_setup_snapshot_id)
    campaign_stage_snapshot_obj = relationship("CampaignStageSnapshot", foreign_keys=campaign_stage_snapshot_id)
    job_type_snapshot_obj = relationship("JobTypeSnapshot", foreign_keys=job_type_snapshot_id)
    status_history = relationship("SubmissionHistory", primaryjoin="Submission.submission_id == SubmissionHistory.submission_id")
    

class SubmissionHistory(Base):
    __tablename__ = "submission_histories"

    submission_id = Column(ForeignKey("submissions.submission_id"), primary_key=True, nullable=False)
    created = Column(DateTime(True), primary_key=True, nullable=False)
    status_id = Column(ForeignKey("submission_statuses.status_id"), nullable=False, index=True)

    submission_obj = relationship("Submission", foreign_keys=submission_id)
    status_type = relationship("SubmissionStatus", primaryjoin="SubmissionHistory.status_id == SubmissionStatus.status_id")


class SubmissionStatus(Base):
    __tablename__ = "submission_statuses"

    status_id = Column(Integer, primary_key=True, nullable=False)
    status = Column(Text, nullable=False)


class CampaignStageSnapshot(Base):
    __tablename__ = "campaign_stage_snapshots"

    campaign_stage_snapshot_id = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('campaign_stage_snapshots_campaign_stage_snapshot_id_seq'::regclass)"),
    )
    campaign_stage_id = Column(ForeignKey("campaign_stages.campaign_stage_id"), nullable=False, index=True)
    experiment = Column(Text, nullable=False)
    name = Column(Text, nullable=False)
    job_type_id = Column(Integer, nullable=False)
    vo_role = Column(Text, nullable=False)
    creator = Column(Integer, nullable=False)
    created = Column(DateTime(True), nullable=False)
    dataset = Column(Text, nullable=False)
    software_version = Column(Text, nullable=False)
    login_setup_id = Column(Integer, nullable=False)
    param_overrides = Column(JSON)
    updater = Column(Integer)
    updated = Column(DateTime(True))
    cs_last_split = Column(Integer)
    cs_split_type = Column(Text)
    cs_split_dimensions = Column(Text)
    completion_type = Column(Text, nullable=False, server_default=text("located"))
    # completion_pct = Column(Text, nullable=False, server_default="95")
    completion_pct = Column(Integer, nullable=False, server_default="95")
    default_clear_cronjob = Column(Boolean, server_default=text("true"), nullable=False)

    campaign_stage = relationship("CampaignStage")


class JobTypeSnapshot(Base):
    __tablename__ = "job_type_snapshots"

    job_type_snapshot_id = Column(
        Integer, primary_key=True, server_default=text("nextval('job_type_snapshots_job_type_snapshot_id_seq'::regclass)")
    )
    job_type_id = Column(ForeignKey("job_types.job_type_id"), nullable=False, index=True)
    name = Column(Text, nullable=False)
    experiment = Column(Text, nullable=False)
    launch_script = Column(Text)
    definition_parameters = Column(JSON)
    input_files_per_job = Column(Integer)
    output_files_per_job = Column(Integer)
    output_file_patterns = Column(Text)
    creator = Column(Integer, nullable=False)
    created = Column(DateTime(True), nullable=False)
    updater = Column(Integer)
    updated = Column(DateTime(True))

    job_type = relationship("JobType")


class LoginSetupSnapshot(Base):
    __tablename__ = "login_setup_snapshots"

    login_setup_snapshot_id = Column(
        Integer, primary_key=True, server_default=text("nextval('login_setup_snapshots_login_setup_id_seq'::regclass)")
    )
    login_setup_id = Column(ForeignKey("login_setups.login_setup_id"), nullable=False, index=True)
    experiment = Column(String(10), nullable=False)
    launch_host = Column(Text, nullable=False)
    launch_account = Column(Text, nullable=False)
    launch_setup = Column(Text, nullable=False)
    creator = Column(Integer, nullable=False)
    created = Column(DateTime(True), nullable=False)
    updater = Column(Integer)
    updated = Column(DateTime(True))
    name = Column(Text, nullable=False)

    login_setup = relationship("LoginSetup")


class RecoveryType(Base):
    __tablename__ = "recovery_types"

    recovery_type_id = Column(
        Integer, primary_key=True, server_default=text("nextval('recovery_types_recovery_type_id_seq'::regclass)")
    )
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=False)


class CampaignRecovery(Base):
    __tablename__ = "campaign_recoveries"

    job_type_id = Column(ForeignKey("job_types.job_type_id"), primary_key=True, nullable=False)
    recovery_type_id = Column(ForeignKey("recovery_types.recovery_type_id"), primary_key=True, nullable=False, index=True)
    recovery_order = Column(Integer, nullable=False, primary_key=True)
    param_overrides = Column(JSON)

    job_type = relationship("JobType")
    recovery_type = relationship("RecoveryType")


class CampaignDependency(Base):
    __tablename__ = "campaign_dependencies"

    campaign_dep_id = Column(Integer, primary_key=True, server_default=text("nextval('campaign_dependency_id_seq'::regclass)"))
    needs_campaign_stage_id = Column(
        ForeignKey("campaign_stages.campaign_stage_id"), primary_key=True, nullable=False, index=True
    )
    provides_campaign_stage_id = Column(
        ForeignKey("campaign_stages.campaign_stage_id"), primary_key=True, nullable=False, index=True
    )
    file_patterns = Column(Text, nullable=False)

    provider = relationship("CampaignStage", foreign_keys=needs_campaign_stage_id, backref="consumer_associations")
    consumer = relationship("CampaignStage", foreign_keys=provides_campaign_stage_id, backref="provider_associations")


class HeldLaunch(Base):
    __tablename__ = "held_launches"
    campaign_stage_id = Column(ForeignKey("campaign_stages.campaign_stage_id"), primary_key=True, nullable=False, index=True)
    created = Column(DateTime(True), nullable=False, primary_key=True)
    parent_submission_id = Column(Integer, nullable=False)
    dataset = Column(Text)
    param_overrides = Column(JSON)
    launcher = Column(Integer, ForeignKey("experimenters.experimenter_id"))
    campaign_stage_obj = relationship("CampaignStage")


class FaultyRequest(Base):
    __tablename__ = "faulty_requests"
    url = Column(Text, nullable=False, primary_key=True)
    status = Column(Integer)
    message = Column(Text)
    ntries = Column(Integer)
    last_seen = Column(DateTime(timezone=True), nullable=False, primary_key=True, server_default=text("now()"))
