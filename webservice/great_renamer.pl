#!/usr/bin/perl 

#
# script to massively rename tables/object classes and common
# variables to make our internal nomenclature match our external one.
#
#
sub rename_em {
    $_ = $_[0];

    s/\bLaunchTemplate\b/LoginSetup/go;
    s/\blaunch_id\b/login_setup_id/go;
    s/\blaunch_definitions\b/login_setups/go;
    # forgot on first round
    s/\blaunch_templates\b/login_setups/go;

    s/\bCampaignDefinition\b/JobType/go;
    s/\bcampaign_definition_id\b/job_type_id/go;
    s/\bcampaign_definitions\b/job_types/go;
    s/\bcampaign_definition_obj\b/job_type_obj/go;

    s/\bCampaignSnapshot\b/CampaignStageSnapshot/go;
    s/\bcampaign_snapshot_id\b/campaign_stage_snapshot_id/go;
    s/\bcampaign_snapshots\b/campaign_stage_snapshots/go;
    s/\bcampaign_snap_obj\b/campaign_stage_snapshot_obj/go;

    s/\bCampaignDefinitionSnapshot\b/JobTypeSnapshot/go;
    s/\bcampaign_definition_snap_id\b/job_type_snapshot_id/go;
    s/\bcampaign_definition_snapshots\b/job_type_snapshots/go;
    s/\bcampaign_definition_snap_obj\b/job_type_snapshot_obj/go;

    s/\bCampaign\b/CampaignStage/go;
    s/\bcampaigns\b/campaign_stages/go;
    s/\bcampaign_id\b/campaign_stage_id/go;
    s/\bneeds_camp_id\b/needs_campaign_stage_id/go;
    s/\buses_camp_id\b/provides_campaign_stage_id/go;
    s/\bcampaign_obj\b/campaign_stage_obj/go;
    s/\bc\b/cs/go;

    s/\bTask\b/Submission/go;
    s/\btasks\b/submissions/go;
    s/\btask_id\b/submission_id/go;
    s/\bparent_task_id\b/parent_submission_id/go;
    s/\btask_obj\b/submission_obj/go;
    s/\btask_parameters\b/submission_parameters/go;
    s/\bt\b/s/go;
    s/\btid\b/sid/go;

    # note this *has* to go last, so we don't go Task->Campaign->CampaignStage
    s/\bTag\b/Campaign/go;
    s/\btags\b/campaigns/go;
    s/\btag_id\b/campaign_id/go;

    s/\bCampaignsTags\b/CampaignCampaignStages/go;
    s/\bcampaigns_tags\b/campaign_campaign_stages/go;

    return $_;
}

$^I='.bak';

while(<>) {
   $_ = rename_em($_);
   print;
}
