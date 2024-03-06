edx-analytics-exporter
======================

# DEPRECATION NOTICE

The Insights product and associated repositories are in the process of being 
deprecated and removed from service. Details on the deprecation status and
process can be found in the relevant [Github issue](https://github.com/openedx/public-engineering/issues/221).

This repository is slated may be archived and moved to the openedx-unsupported
Github organization at any time.

The following sections are for historical purposes only.

---

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
