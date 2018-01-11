#!/usr/bin/env python

"""
Export course data.

Usage:
  course-exporter [options] <config> <org-config> [--env=<environment>...] [--course=<course>...] [--task=<task>...]

Arguments:
  <config>                   YAML configuration file.
  <org-config>               YAML organization configuration file.
  --env=<environment>        Select environment. Can be specified multiple times.
  --task=<task>              Select task. Can be specified multiple times.
  --course=<course>             Select course. Can be specified multiple times.

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


  --auth-file=<file>         Authentication file with credentials.

  --django-admin=<admin>     The path to the appropriate django-admin.py
  --django-pythonpath=<path> The django python path
"""


from contextlib import contextmanager
import datetime
import os
import subprocess
import logging
import logging.config
import re

from opaque_keys.edx.keys import CourseKey

from exporter.tasks import CourseTask, FatalTaskError
from exporter.main import run_tasks, archive_directory, upload_data, get_all_courses
from exporter.config import setup, get_config_for_env, get_config_for_course
from exporter.util import make_temp_directory, with_temp_directory, merge

log = logging.getLogger(__name__)


def main():
    general_config = setup(__doc__)

    courses_with_env = get_courses_with_env(general_config)

    for course in general_config['values']['course']:
        config = get_config_for_course(general_config, course)

        with make_course_directory(config, course) as temp_directory:
            results = export_course_data(config, temp_directory, courses_with_env[course])
            upload_files(config, temp_directory)

def get_courses_with_env(config):
    courses_with_env = {}
    courses = config['values']['course']

    for environment in config['environments']:
         kwargs = merge(config['values'], config['environments'][environment])
         all_courses  = get_all_courses(**kwargs)

         if all_courses:
             found_courses = set(courses) & set(all_courses)
             courses_with_env.update({course: environment for course in found_courses})
             courses = set(courses) - found_courses

    if courses:
        log.error("Failed to find courses: %s", list(courses))
        raise FatalTaskError("Failed to find courses in configured environments.")

    return courses_with_env

def export_course_data(config, destination, environment):
    log.info('Exporting data for %s', config['course'])

    results = []

    kwargs = get_config_for_env(config, environment)
    kwargs['work_dir'] = destination

    log.info("Getting data for course %s", config['course'])
    filenames = run_tasks(CourseTask, **kwargs)
    results.extend(filenames)

    return results

def upload_files(config, results_directory):
    bucket = config['output_bucket']
    prefix = config['output_prefix'] or ''
    filename_safe_course_id = get_filename_safe_course_id(config['course'])
    output_date = str(datetime.date.today())

    for filename in os.listdir(results_directory):
        filepath = os.path.join(results_directory, filename)

        target = 's3://{bucket}/{prefix}{course}/state/{date}/{name}'.format(
            bucket=bucket,
            prefix=prefix,
            course=filename_safe_course_id,
            date=output_date,
            name=filename
        )

        log.info('Uploading file %s to %s', filepath, target)

        cmd = 'aws s3 cp --acl bucket-owner-full-control {filepath} {target}'
        cmd = cmd.format(filepath=filepath, target=target)

        if not config['dry_run']:
            subprocess.check_call(cmd, shell=True)
        else:
            log.info('cmd: %s', cmd)

@contextmanager
def make_course_directory(config, course):
    filename_safe_course = get_filename_safe_course_id(course)
    course_dir = config['work_dir']

    prefix = '{0}_'.format(filename_safe_course)

    with make_temp_directory(prefix=prefix, directory=course_dir) as temp_dir:
        # create working directory
        today = str(datetime.date.today())
        dir_name = '{name}-{date}'.format(name=filename_safe_course, date=today)
        course_dir = os.path.join(temp_dir, dir_name)
        os.mkdir(course_dir)

        yield course_dir

def get_filename_safe_course_id(course_id, replacement_char='_'):
    """
    Create a representation of a course_id that can be used safely in a filepath.
    """
    try:
        course_key = CourseKey.from_string(course_id)
        filename = unicode(replacement_char).join([course_key.org, course_key.course, course_key.run])
    except InvalidKeyError:
        # If the course_id doesn't parse, we will still return a value here.
        filename = course_id

    # The safest characters are A-Z, a-z, 0-9, <underscore>, <period> and <hyphen>.
    # We represent the first four with \w.
    # TODO: Once we support courses with unicode characters, we will need to revisit this.
    return re.sub(r'[^\w\.\-]', unicode(replacement_char), filename)
