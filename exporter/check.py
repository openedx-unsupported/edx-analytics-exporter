#!/usr/bin/env python

"""
Check the most recent export to ensure that all expected files were created properly. This script will exit with a non-
zero status if and only if it detects a problem.

Usage:
    exporter-check [options] <config> <org-config>

Arguments:
    <config>                 YAML configuration file.
    <org-config>             YAML organization configuration file.

Options:
    -h --help                Show this screen.
    -n --dry-run             Don't run anything, just show what would be done.
    --window=<days>          Fail if the most recent file is older than this many days. [default: 6]
"""

import datetime
import logging
import os
import json
import subprocess
import sys

from exporter.config import setup, get_config_for_org


log = logging.getLogger(__name__)


# pylint: disable=missing-docstring


def main():
    general_config = setup(__doc__)

    return check_export(general_config)


def check_export(general_config):
    start_timestamp = datetime.datetime.utcnow()

    bucket_file_metadata = {}
    most_recent_file_per_org = {}
    for organization in general_config['organizations']:
        config = get_config_for_org(general_config, organization)

        if not config['monitor']:
            continue

        bucket = config['output_bucket']
        if bucket not in bucket_file_metadata:
            bucket_file_metadata[bucket] = get_bucket_file_list(bucket)

        for metadata in bucket_file_metadata[bucket]:
            if metadata.organization != organization:
                continue

            most_recent_file = most_recent_file_per_org.get(metadata.organization, None)
            if not most_recent_file or metadata.timestamp > most_recent_file.timestamp:
                most_recent_file_per_org[metadata.organization] = metadata

    min_date = start_timestamp - datetime.timedelta(days=int(general_config['values']['window']))
    failed = False
    for organization in general_config['organizations']:
        config = get_config_for_org(general_config, organization)
        if not config['monitor']:
            log.info('Ignoring organization %s', organization)
            continue

        most_recent_file = most_recent_file_per_org.get(organization)
        if not most_recent_file:
            log.error("Missing file for organization '%s', no files found for this organization.", organization)
            failed = True
        elif most_recent_file.timestamp < min_date:
            log.error(
                "Missing file for organization '%s', most recent file is s3://%s/%s, last modified %s",
                organization,
                most_recent_file.bucket,
                most_recent_file.filename,
                most_recent_file.timestamp.isoformat()
            )
            failed = True
        else:
            log.info(
                'Organization %s OK, most recent file is s3://%s/%s, last modified %s',
                organization,
                most_recent_file.bucket,
                most_recent_file.filename,
                most_recent_file.timestamp.isoformat()
            )

    return 0 if not failed else 1


def get_bucket_file_list(bucket):
    response = subprocess.check_output("aws s3api list-objects --bucket {}".format(bucket), shell=True)
    # Output is a JSON blob with a "Contents" field which is an array of structures that describe each file
    parsed_response = json.loads(response)
    metadata_list = []
    for obj in parsed_response['Contents']:
        # Parse each object and add it to the list if it is relevant
        metadata = ExportedFileMetadata.from_json(bucket, obj)
        if metadata:
            metadata_list.append(metadata)
    return metadata_list


class ExportedFileMetadata(object):

    def __init__(self, **kwargs):
        self.bucket = kwargs.get('bucket', '')
        self.timestamp = kwargs.get('timestamp', None)
        self.size = kwargs.get('size', 0)
        self.filename = kwargs.get('filename', '')
        self.organization = kwargs.get('organization', '')

    @staticmethod
    def from_json(bucket, obj):
        kwargs = {
            'bucket': bucket
        }

        filename = kwargs['filename'] = obj['Key']
        _, extension = os.path.splitext(filename)
        if extension == '.zip' and '/' not in filename:
            kwargs['organization'] = filename.split('-')[0]
        else:
            return None

        kwargs['size'] = obj['Size']
        kwargs['timestamp'] = datetime.datetime.strptime(obj['LastModified'], "%Y-%m-%dT%H:%M:%S.%fZ")

        return ExportedFileMetadata(**kwargs)


if __name__ == '__main__':
    sys.exit(main())
