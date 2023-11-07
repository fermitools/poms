---
layout: page
title: Variable Substitution
---
* TOC
{:toc}
When POMS launches jobs, after it builds the full 'ssh user@hostname "setup_stuff; launch_command"',
it performs python '%' substitution with some values. More exactly, the following strings will
be replaced:

**%(dataset)s** The dataset for this step of execution -- either the overall dataset for the campaign, the result of partitioning the overall Campaign's input dataset in the first step of a partitioned workflow, or the dataset defined by POMS to be the appropriate child files of the previous steps inputs.

**%(version)s** The version string from the Campaign or Campaign Stage

**%(group)s** The experiment/group we are currentlyworking on behalf of.

**%(experimenter)s** The person who hit the launch button on the submission.

For the launch command itself, the dictionary of campaign keywords is also substituted in.

See also the discussion of [Namespaces]({{ site.url }}/docs/internals/namespaces).