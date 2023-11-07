---
layout: page
title: Analysis Support
---
* TOC
{:toc}
Today we had the meeting with Grid Security, Jobsub developers and Fife leadership
to discuss how to support job launches for Analysis users.

After discussion, we concluded that the following approach would be used:

    1. Analysis launches will run from the POMS server, and will not ssh into a gpvm node, etc.
    2. They will use proxies users will upload to POMS for authentication.
    3. Files needed for launch would either be in the gpvm bluearc mounts, or
       pre-uploaded to POMS by the user.

This implies several additional facilities for POMS:

    1. A proxy upload service, with a poms_client addition to use it.
    2. A launch-support-file upload service, with a poms_client addition/browser file upload page.
    3. A fixed Launch/Setup that analysis jobs will use and be unable to change.

For the latter, I am thinking we should ssh back into the POMS server into the poms_launcher account
to actually do the launch, making sure the files the user had uploaded are visible to that launch session.

To do that, we'll make a "sandbox" directory for each launch, with hard links to the users files,
and give the name of that sandbox (only) to that launch script. As long as the parent directory of the sandboxes is executable but not readable by the poms_launcher account, it can't scan them to guess sandbox directories.

issue #21502 is the parent ticket to track the changes needed for this.