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
