---
layout: page
title: Data Dictionary Cheat Sheet
---
* TOC
{:toc}
Until we rename all the tables, etc. we need to what the terms we're using in the UI map to what tables, etc.

<table class="table table-striped table-bordered">
    <tr>
        <th>What users call it</th>
        <th>ORM Name</th>
        <th>Table Name</th>
    </tr>
    <tr>
        <td>Tagged Campaign</td>
        <td>Tag</td>
        <td>tags</td>
    </tr>
    <tr>
        <td>Campaign Stage</td>
        <td>Campaign</td>
        <td>campaigns</td>
    </tr>
    <tr>
        <td>Job Type</td>
        <td>CampaignDefinition</td>
        <td>campagin_definitions</td>
    </tr>
    <tr>
        <td>Launch/Login Template</td>
        <td>LaunchTemplate</td>
        <td>launch_template</td>
    </tr>
    <tr>
        <td>Submission</td>
        <td>Task</td>
        <td>tasks</td>
    </tr>
    <tr>
        <td>Job</td>
        <td>Job</td>
        <td>jobs</td>
    </tr>
    <tr>
        <td>Files</td>
        <td>JobFiles</td>
        <td>job_files</td>
    </tr>
</table>


Several of the above have snapshots for history so Campaign's have CampaignSnapshots -> campaign_snapshot;
and Jobs have JobHistory/job_histories

So a *(Tagged) Campaign* is some bunch of processing an experiment wants to do. It is broken down
into 1 or more *Campaign Stages* which are jobs that do a certain specific kind of work in the overall
Campaign, and which one or more Submissions of Jobs will be made (one Submission can of course launch
multiple Jobs). 

A Campaign may be *Partitioned* by setting a *Split Type* on the first Campaign Stage in the (Tagged) Campaign, 
which will  break up its input dataset into one or more smaller datasets.  Each successive launch of that lead 
campaign stage will pull the next split-off dataset chunk for processing.

Campaign Stages may depend on each other, which means that when one completes, this triggers a launch of the other, with a dataset defined based on the output of the first.  Currently this is defined in the GUI on the downstream campaign stage, specifying what it depends upon -- or in the overall Campaign Editor by a link between stages.

To make the partitioning work smoothly, campaign stages may depend *on themselves* to trigger a new launch as each one completes.   Or you can link workflow stages into a cycle to cause the first stage to be launched when a whole pass through the workflow completes.
