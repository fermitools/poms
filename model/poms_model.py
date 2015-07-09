# coding: utf-8
from sqlalchemy import BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String, Text, text
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class Campaign(Base):
    __tablename__ = 'campaigns'

    campaign_id = Column(Integer, primary_key=True, server_default=text("nextval('campaigns_campaign_id_seq'::regclass)"))
    experiment = Column(ForeignKey(u'experiments.experiment'), nullable=False, index=True)
    task_definition_id = Column(ForeignKey(u'task_definitions.task_definition_id'), nullable=False, index=True, server_default=text("nextval('campaigns_task_definition_id_seq'::regclass)"))
    creator = Column(ForeignKey(u'experimenters.experimenter_id'), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    updater = Column(Integer)
    updated = Column(DateTime(True))

    experimenter = relationship(u'Experimenter')
    experiment1 = relationship(u'Experiment')
    task_definition = relationship(u'TaskDefinition')


class Experimenter(Base):
    __tablename__ = 'experimenters'

    experimenter_id = Column(Integer, primary_key=True, server_default=text("nextval('experimenters_experimenter_id_seq'::regclass)"))
    first_name = Column(Text, nullable=False)
    last_name = Column(Text)
    email = Column(Text, nullable=False)


class Experiment(Base):
    __tablename__ = 'experiments'

    experiment = Column(String(10), primary_key=True)
    name = Column(Text, nullable=False)


class ExperimentsExperimenter(Base):
    __tablename__ = 'experiments_experimenters'

    experiment = Column(ForeignKey(u'experiments.experiment'), primary_key=True, nullable=False, index=True)
    experimenter_id = Column(ForeignKey(u'experimenters.experimenter_id'), primary_key=True, nullable=False)
    active = Column(Boolean, nullable=False, server_default=text("true"))

    experiment1 = relationship(u'Experiment')
    experimenter = relationship(u'Experimenter')


class Job(Base):
    __tablename__ = 'jobs'

    job_id_ = Column('job_id ', BigInteger, primary_key=True, server_default=text("nextval('jobs_job_id_seq'::regclass)"))
    task_id = Column(ForeignKey(u'tasks.task_id'), nullable=False, index=True)
    node_name = Column(Text, nullable=False)
    cpu_type = Column(Text, nullable=False)
    host_site = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    updated = Column(DateTime(True), nullable=False)

    task = relationship(u'Task')


class ServiceDowntime(Base):
    __tablename__ = 'service_downtimes'

    service_id = Column(ForeignKey(u'services.service_id'), primary_key=True, nullable=False)
    downtime_started = Column(DateTime(True), primary_key=True, nullable=False)
    downtime_ended = Column(DateTime(True), nullable=True)

    service = relationship(u'Service')


class Service(Base):
    __tablename__ = 'services'

    service_id = Column(Integer, primary_key=True, server_default=text("nextval('services_service_id_seq'::regclass)"))
    name = Column(Text, nullable=False)
    host_site = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    updated = Column(DateTime(True), nullable=False)
    active = Column(Boolean, nullable=False, server_default=text("true"))
    parent_service_id = Column(ForeignKey(u'services.service_id'), index=True)
    url = Column(Text)

    parent_service = relationship(u'Service', remote_side=[service_id])


class TaskDefinition(Base):
    __tablename__ = 'task_definitions'

    task_definition_id = Column(Integer, primary_key=True, server_default=text("nextval('task_definitions_task_definition_id_seq'::regclass)"))
    name = Column(Text, nullable=False, unique=True)
    experiment = Column(ForeignKey(u'experiments.experiment'), nullable=False, index=True)
    launch_script = Column(Text)
    definition_parameters = Column(JSON)
    input_files_per_job = Column(Integer)
    output_files_per_job = Column(Integer)
    creator = Column(ForeignKey(u'experimenters.experimenter_id'), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    updater = Column(ForeignKey(u'experimenters.experimenter_id'), index=True)
    updated = Column(DateTime(True))

    experimenter = relationship(u'Experimenter', primaryjoin='TaskDefinition.creator == Experimenter.experimenter_id')
    experiment1 = relationship(u'Experiment')
    experimenter1 = relationship(u'Experimenter', primaryjoin='TaskDefinition.updater == Experimenter.experimenter_id')


class Task(Base):
    __tablename__ = 'tasks'

    task_id = Column(Integer, primary_key=True, server_default=text("nextval('tasks_task_id_seq'::regclass)"))
    campaign_id = Column(ForeignKey(u'campaigns.campaign_id'), nullable=False, index=True, server_default=text("nextval('tasks_campaign_id_seq'::regclass)"))
    task_definition_id = Column(Integer, nullable=False, index=True, server_default=text("nextval('tasks_task_definition_id_seq'::regclass)"))
    task_order = Column(Integer, nullable=False)
    input_dataset = Column(Text, nullable=False)
    output_dataset = Column(Text, nullable=False)
    creator = Column(ForeignKey(u'experimenters.experimenter_id'), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    status = Column(Text, nullable=False)
    task_parameters = Column(JSON)
    waiting_for = Column(ForeignKey(u'tasks.task_id'), index=True)
    waiting_threshold = Column(Integer)
    updater = Column(ForeignKey(u'experimenters.experimenter_id'), index=True)
    updated = Column(DateTime(True))

    campaign = relationship(u'Campaign')
    experimenter = relationship(u'Experimenter', primaryjoin='Task.creator == Experimenter.experimenter_id')
    experimenter1 = relationship(u'Experimenter', primaryjoin='Task.updater == Experimenter.experimenter_id')
    parent = relationship(u'Task', remote_side=[task_id])
