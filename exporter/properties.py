#!/usr/bin/env python

"""
Create a property file per organization in the specified directory.

Each file contains a list of KEY=VALUE pairs for the organization
according to the configuration.

Only the ORG and BUCKET keys are currently being exported. These
values can be used by Jenkins jobs to start multiple exporter tasks,
using each property file for the task parameters.

Usage:
  exporter-properties [options] [--include=<file>...] <config> <directory>

Arguments:
  <config>                 YAML configuration file.
  <directory>              Directory where to save Jenkins property files.

Options:
  -h --help                Show this screen.
  -n --dry-run             Don't run anything, just show what would be done.
  --output-bucket=<bucket> Destination S3 bucket.
  --include=<file>         Include data from file in property file.
  --orgs=<orgs>            Space separated list of organization identifiers.
                           Can use wildcards.
"""

import os.path
import shutil
import sys
from fnmatch import fnmatch

from docopt import docopt

from exporter.config import setup, get_config_for_org


def main():
    program_options = docopt(__doc__)
    config = setup(__doc__)

    directory = program_options['<directory>']
    orgs = program_options['--orgs']
    files = program_options['--include']
    bucket = program_options['--output-bucket']

    export_properties(config, directory, files, orgs)


def export_properties(config, directory, files=None, orgs=None):
    recreate_directory(directory)

    orgs = [o.lower() for o in orgs.split()] if orgs else ['*']
    print orgs

    files_data = load_files(files)

    for organization in config['organizations']:
        org_config = get_config_for_org(config, organization)

        bucket = org_config['output_bucket']
        organization = organization.lower()

        if any(fnmatch(organization, org) for org in orgs):
            filename = os.path.join(directory, organization)
            with open(filename, 'w') as f:
                f.write('ORG={}\n'.format(organization))
                f.write('OUTPUT_BUCKET={}\n'.format(bucket))
                f.write(files_data)


def recreate_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.mkdir(directory)


def load_files(files):
    values = []
    for filename in files:
        with open(filename) as f:
            data = f.read()
            if not data.endswith('\n'):
                data += '\n'
            values.append(data)

    return ''.join(values)


if __name__ == '__main__':
    sys.exit(main())
