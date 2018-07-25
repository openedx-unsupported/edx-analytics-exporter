*Note*
This is a copy of the environment/production branch.  This branch will exist while we are transitioning the services from the Admin jenkins box to the new jenkins box.  This branch is needed because the historical running environment relied upon a specific /mnt/ephemeral-01 working path and the ability to execute jobs using SUDO.  The new running environment uses a local working path and does not rely upon SUDO. 

Any commit merges pulled down into this branch must respect the historical working path and inclusion of SUDO.


edx-analytics-exporter
===============

This is a set of analytics tasks to split selected database tables for export to
partners. We use this at edX to deliver data packages to our
partners. edx-analytics-pipeline contains the jobs used to split events logs.

We have open sourced this since a few Open edX community members have asked for
it. That said, we want to eventually merge this functionality into
edx-analytics-pipeline, refactoring and adding tests as part of that
process. Use this code at your own peril.


Installing
==========

Assuming you have virtualenv and virtualenv-wrapper installed, from the 
project root run:

```
mkvirtualenv analytic-exporter
pip install -r github_requirements.txt
pip install -e .
```

Running
=======

Sorry, we don't have docs for this yet -- we plan to address that as we 
integrate this into the pipeline. 


Contributions
============

Contributions welcome, though see note above. See https://open.edx.org/
