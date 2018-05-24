#!/usr/bin/perl 

#
# script to massively rename tables/object classes and common
# variables to make our internal nomenclature match our external one.
#
#
sub rename_em {
    $_ = $_[0];
    s/\bCampaignDefinition\b/JobType/go;
    s/\bcampaign_definition_id\b/job_type_id/go;
    s/\bcampaign_definitions\b/job_types/go;
    # ALTER TABLE campaign_definitions RENAME TO job_types;
    # ALTER TABLE job_types RENAME COLUMN campaign_definition_id TO job_types_id;
    s/\bcampaign_definition_obj\b/job_type_obj/go;

    s/\bCampaignSnapshot\b/CampaignStageSnapshot/go;
    s/\bcampaign_snapshot_id\b/campaign_stage_snapshot_id/go;
    s/\bcampaign_snapshots\b/campaign_stage_snapshots/go;
    # ALTER TABLE campaign_snapshots RENAME TO campaign_stage_snapshots;
    # ALTER TABLE campaign_stage_snapshots RENAME COLUMN campaign_snapshot_id TO campaign_stage_snapshot_id;
    s/\bcampaign_snap_obj\b/campaign_stage_snapshot_obj/go;

    s/\bCampaignDefinitionSnapshot\b/JobTypeSnapshot/go;
    s/\bcampaign_definition_snap_id\b/job_type_snapshot_id/go;
    s/\bcampaign_definition_snapshots\b/job_type_snapshots/go;
    # ALTER TABLE campaign_definition_snapshots RENAME TO job_type_snapshots;
    # ALTER TABLE job_type_snapshots RENAME COLUMN campaign_definition_snap_id  TO job_type_snapshot_id;
    s/\bcampaign_definition_snap_obj\b/job_type_snapshot_obj/go;

    s/\bCampaign\b/CampaignStage/go;
    s/\bcampaigns\b/campaign_stages/go;
    s/\bcampaign_id\b/campaign_stage_id/go;
    # ALTER TABLE campaigns RENAME TO campaign_stages;
    # ALTER TABLE campaign_stages RENAME COLUMN campaign_id to campaign_stage_id;
    s/\bcampaign_obj\b/campaign_stage_obj/go;
    s/\bc\b/cs/go;

    s/\bTask\b/Submission/go;
    s/\btasks\b/submissions/go;
    s/\btask_id\b/submission_id/go;
    # ALTER TABLE tasks RENAME to submissions;
    # ALTER TABLE submissions RENAME COLUMN task_id TO submission_id;
    s/\btask_obj\b/submission_obj/go;
    s/\btask_params\b/submission_params/go;
    # ALTER TABLE submissions RENAME COLUMN task_params TO submission_params;
    s/\bt\b/s/go;
    s/\btid\b/sid/go;

    # note this *has* to go last, so we don't go Task->Campaign->CampaignStage
    s/\bTag\b/Campaign/go;
    s/\btags\b/campaigns/go;
    s/\btag_id\b/campaign_id/go;
    # ALTER TABLE tags RENAME TO campaigns;
    # ALTER TABLE campaigns RENAME COLUMN tag_id to campaign_id;

    s/\bCampaignsTags\b/CampaignsCampaignStages/go;
    s/\bcampaigns_tags\b/campaigns_campaign_stages/go;
    # ALTER TABLE campaigns_tags RENAME TO campaigns_campaign_stages;

    return $_;
}

$^I='.bak';

while(<>) {
   $_ = rename_em($_);
   print;
}
