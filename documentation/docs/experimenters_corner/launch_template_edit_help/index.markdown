---
layout: page
title: Launch Template Edit Help
---
* TOC
{:toc}


This page lets you configure a launch template, which specifies where you log in and what you setup to launch jobs.

First you pick your experiment from the pulldown, then you fill out the form, and hit "Submit"

The account in question will need the following .k5login entries:

    poms/cd/fermicloud045.fnal.gov@FNAL.GOV
    poms/cd/pomsgpvm01.fnal.gov@FNAL.GOV
    <YOUR KERBEROS USERNAME>@FNAL.GOV

You MUST add your default principal to the .k5login file if the .k5login file exists. So if you're creating a .k5login file for the first time don't forget to add your account name in addition to the poms ids. Your Fermilab username must be lower case and the FNAL.GOV must be uppercase.

##### Name

Campaign identifying string, something like "Generic experiment X setup q1_2_3 v1.2.3"

##### Host

Host name like "some_node.fnal.gov"

##### Account

Account name

##### Setup

A set of shell commands setting up the environment for campaign to run. This will have [Variable Substitution]({{ site.url }}/docs/internals/variable_substitution)   performed.