#!/usr/bin/env python

"""
Export course data.

Usage:
  exporter [options] <config> [--env=<environment>...] [--org=<organization>...] [--task=<task>...]

Arguments:
  <config>                   YAML configuration file.
  --env=<environment>        Select environment. Can be specified multiple times.
  --org=<organization>       Select organization. Can be specified multiple times.
  --task=<task>              Select task. Can be specified multiple times.

Options:
  -h --help                  Show this screen.
  -n --dry-run               Don't run anything, just show what would be done.

  --work-dir=<dir>           Working directory.

  --limit=<limit>            Maximum number of results per file.

  --output-bucket=<bucket>   Destination bucket.
  --output-prefix=<pfx>      Prefix all output key names with this string.

  --external-prefix=<pfx>    Prefix relative paths to external files with this string.
  --pipeline-bucket=<pb>     Bucket that the EMR pipeline drops files in.
  --se-bucket=<bucket>       The S3 bucket to retrieve StackExchange data from.


  --gpg-keys=<dir>           Directory with gpg keys.
  --auth-file=<file>         Authentication file with credentials.

  --django-admin=<admin>     The path to the appropriate django-admin.py
  --django-pythonpath=<path> The django python path
"""

from contextlib import contextmanager
from copy import copy
import datetime
from distutils.spawn import find_executable  # pylint: disable=import-error, no-name-in-module
import logging
import logging.config
import os
import sys
import subprocess
import tempfile

import gnupg

from opaque_keys.edx.keys import CourseKey

from exporter.config import setup, get_config_for_org, get_config_for_env
from exporter.tasks import OrgTask, CourseTask
from exporter.tasks import FindAllCoursesTask
from exporter.tasks import FatalTaskError
from exporter.tasks import DEFAULT_TASKS
from exporter.tasks import OrgEmailOptInTask
from exporter.util import make_temp_directory, with_temp_directory
from exporter.util import filter_keys, memoize, execute_shell
from exporter.util import logging_streams_on_failure


log = logging.getLogger(__name__)


# pylint: disable=missing-docstring


MAX_TRIES_FOR_DATA_UPLOAD = 5


def main():
    general_config = setup(__doc__)
    for organization in general_config['organizations']:

        config = get_config_for_org(general_config, organization)

        with make_org_directory(config, organization) as destination:
            results = export_organization_data(config, destination)
            encrypt_files(config, results)
            archive = archive_directory(config, destination)
            upload_data(config, archive)


def export_organization_data(config, destination):
    log.info('Exporting data for %s', config['organization'])

    results = []
    for environment in config['environments']:
        log.info("Using environment %s", environment)

        kwargs = get_config_for_env(config, environment)
        kwargs['work_dir'] = destination

        courses = get_org_courses(**kwargs)
        kwargs['courses'] = courses

        filenames = run_tasks(OrgTask, **kwargs)
        results.extend(filenames)

        for course in courses:
            log.info("Getting data for course %s", course)
            filenames = run_tasks(CourseTask, course=course, **kwargs)
            results.extend(filenames)

    return results


def run_tasks(task_cls, **kwargs):
    results = []

    available_tasks = {t.__name__.lower(): t for t in DEFAULT_TASKS if issubclass(t, task_cls)}
    requested_tasks = [name.lower() for name in kwargs.get('tasks', [])]
    selected_tasks = [t for t in filter_keys(available_tasks, requested_tasks).values() if t]

    original_kwargs = kwargs

    for task in selected_tasks:
        # Prevent tasks from overwriting the original arguments
        kwargs = copy(original_kwargs)

        # Skip OrgEmailOptInTask in edge
        # TODO: make skip tasks configurable
        if task is OrgEmailOptInTask and kwargs['environment'] == 'edge':
            log.info("Ignoring task %s", task.__name__)
            continue

        log.info("Running task %s", task.__name__)

        # Get filename, and make sure its directory exists (in case it's a subdirectory).
        filename = task.get_filename(**kwargs)
        file_dir = os.path.dirname(filename)
        if not os.path.isdir(file_dir):
            os.mkdir(file_dir)

        try:
            with logging_streams_on_failure(task.__name__) as (output_file, error_file):
                task.run(filename, stderr_file=error_file, stdout_file=output_file, **kwargs)
                log.info('Saving task results to %s', filename)
                results.append(filename)
        except FatalTaskError:
            log.exception('Task %s failed fatally to write to %s', task.__name__, filename)
            raise
        except Exception:  # pylint: disable=broad-except
            failed_filename = filename + '.failed'
            log.exception('Task %s failed fatally, writing failure file %s', task.__name__, failed_filename)

            # Remove the failed file if it exists
            if os.path.exists(filename):
                os.remove(filename)

            # Create placeholder a file with error message
            with open(failed_filename, 'w') as failure_file:
                failure_file.write('An error occurred generating this file.\n')

            results.append(failed_filename)

    return results


@with_temp_directory
def encrypt_files(config, filenames, temp_directory=None):
    dry_run = config['dry_run']

    # collect all recipients
    recipients = config.get('recipients')
    if not recipients:
        recipients = [config['recipient']]
    if 'gpg_master_key' in config:
        recipients.append(config['gpg_master_key'])

    # user the temp directory, if specified, to store the keyring
    gpg = gnupg.GPG(gnupghome=temp_directory)
    gpg_key_dir = config['gpg_keys']
    gpg.encoding = 'utf-8'

    for recipient in recipients:
        # import recipient gpg key
        log.info('Using gpg key for %s', recipient)
        gpg_key_file = os.path.join(gpg_key_dir, recipient)
        with open(gpg_key_file, 'r') as gpg_key_file:
            gpg.import_keys(gpg_key_file.read())

    results = []
    for filepath in filenames:
        if not os.path.exists(filepath):
            log.info('Skipping missing file %s', filepath)
            continue

        log.info('Encrypting file %s', filepath)
        encrypted_filepath = '{0}.gpg'.format(filepath)
        if not dry_run:
            with open(filepath) as input_file:
                gpg.encrypt_file(
                    input_file,
                    recipients,
                    always_trust=True,
                    output=encrypted_filepath,
                    armor=False,
                )
            # delete original file even if it was not encrypted
            os.remove(filepath)
        else:
            log.info('Producing encrypted file %s', encrypted_filepath)

        results.append(encrypted_filepath)

    return results


def archive_directory(config, directory):
    root_dir = os.path.dirname(directory)
    base_dir = os.path.basename(directory)

    if not find_executable("zip"):
        raise FatalTaskError("The analytics exporter requires zip on the PATH.")

    log.info('Archiving %s', directory)

    # The ZIP mechanics must support archives over 4GB. For example, the
    # function `shutil.make_archive` does not support them.

    cmd = ('cd {root_dir} ; '
           'zip --quiet --recurse-paths {base_dir}.zip {base_dir} ; '
           'cd {directory}')
    cmd = cmd.format(root_dir=root_dir, base_dir=base_dir, directory=directory)

    if not config['dry_run']:
        subprocess.check_call(cmd, shell=True)
    else:
        log.info('cmd: %s', cmd)

    archive = os.path.join(root_dir, base_dir + '.zip')

    log.info('Save zip file to %s', archive)

    return archive


def upload_data(config, filepath):
    bucket = config['output_bucket']
    prefix = config['output_prefix'] or ''
    name = os.path.basename(filepath)
    target = 's3://{bucket}/{prefix}{name}'.format(bucket=bucket, prefix=prefix, name=name)

    log.info('Uploading file %s to %s', filepath, target)

    cmd = 'aws s3 cp --acl bucket-owner-full-control {filepath} {target}'
    cmd = cmd.format(filepath=filepath, target=target)

    if not config['dry_run']:
        local_kwargs = {'max_tries': MAX_TRIES_FOR_DATA_UPLOAD}
        execute_shell(cmd, **local_kwargs)
    else:
        log.info('cmd: %s', cmd)

    return target


def get_org_courses(organization, **kwargs):
    # if no courses specified, get all courses.
    courses = kwargs.get('courses', [])
    all_courses = get_all_courses(**kwargs)

    if courses and all_courses:
        # use only courses that exists
        courses = set(courses) & set(all_courses)
    elif all_courses:
        # otherwise use all courses
        courses = all_courses

    # select only courses for the relevant organization
    organization_names = [organization] + kwargs.get('other_names', [])
    courses = filter_courses(courses, organization_names)

    # sort and remove duplicates
    courses = sorted(set(courses))

    if courses:
        log.info('Courses for %r: %r', organization_names, courses)
    else:
        log.info('No courses found for %r', organization_names)

    return courses


def filter_courses(courses, organization_names):
    """
    Select courses that belong to the requested organizations.
    Case-insensitve.

    """

    organization_names = [org.lower() for org in organization_names]

    def match(course):
        course_key = CourseKey.from_string(course)
        course_organization = course_key.org.lower()
        return course_organization in organization_names

    courses = [c for c in courses if match(c)]

    return courses


def get_all_courses(**kwargs):
    log.info('Retrieving all courses')

    # make a set of fixed arguments, so we can memoize
    kwargs = {k: v for k, v in kwargs.iteritems() if k.startswith('django')}
    kwargs['dry_run'] = False  # always query for course names
    kwargs['limit'] = False  # don't limit number of courses

    courses = _find_all_courses(**kwargs)

    return courses


@memoize
def _find_all_courses(**kwargs):
    # get all courses using task, saving to a temp file
    with tempfile.NamedTemporaryFile() as temp:
        with logging_streams_on_failure('Find All Courses') as (output_file, error_file):
            try:
                FindAllCoursesTask.run(temp.name, stderr_file=error_file, stdout_file=output_file, **kwargs)
            except:  # pylint: disable=bare-except
                courses = []
                log.warning('Failed to retrieve list of all courses.', exc_info=True)
            else:
                temp.seek(0)
                lines = (l.strip() for l in temp.readlines())
                courses = [l for l in lines if l]
                log.debug("Found courses: %s", courses)
    return courses


@contextmanager
def make_org_directory(config, organization):
    org_dir = config['work_dir']

    prefix = '{0}_'.format(organization)

    with make_temp_directory(prefix=prefix, directory=org_dir) as temp_dir:
        # create working directory
        today = str(datetime.date.today())
        dir_name = '{name}-{date}'.format(name=organization, date=today)
        org_dir = os.path.join(temp_dir, dir_name)
        os.mkdir(org_dir)

        yield org_dir


if __name__ == '__main__':
    sys.exit(main())
