# pylint: disable=missing-docstring

import json
import logging
import logging.config
import os
import tempfile

from docopt import docopt
import yaml

from exporter.util import merge, filter_keys
import six


WORK_SUBDIR = 'course-data'

log = logging.getLogger(__name__)


def setup(doc, argv=None):
    program_options = docopt(doc, argv=argv)
    setup_logging()

    log.info('Reading configuration')

    return _get_config(program_options)


def _get_config(program_options):
    with open(program_options['<config>']) as f:
        config = yaml.load(f)

    if '<org-config>' in program_options:
        # org-config is not passed in separately for all jobs and is not available to jobs that run as a "slave"
        with open(program_options['<org-config>']) as f:
            org_config = yaml.load(f)

        config['organizations'] = org_config['organizations']

    update_config(config, program_options)

    # modify work directory for this command
    work_dir = os.path.join(config['values']['work_dir'], WORK_SUBDIR)
    config['values']['work_dir'] = work_dir

    return config


def update_config(config, program_options):
    merge_program_options(config, program_options)
    update_values(config)
    update_environments(config)
    # Config files may not always contain organization information.
    if 'organizations' in config:
        update_organizations(config)
    update_tasks(config)


def merge_program_options(config, program_options):
    # get program options, removing '--' and replacing '-' with '_'
    options = {k[2:].replace('-', '_'): v for k, v
               in six.iteritems(program_options)
               if k.startswith('--')}

    config['options'] = options


def update_values(config):
    # override defaults values with program options
    values = merge(config['options'], config['defaults'])

    # set defaults if missing
    if not values.get('work_dir'):
        values['work_dir'] = tempfile.gettempdir()

    config['values'] = values


def update_environments(config):
    values = config['values']

    # select only the organizations requested
    environments = filter_keys(config['environments'], values.get('env'))

    # read authentication tokens
    tokens = {}
    auth_filename = values.get('auth_file')
    if auth_filename:
        auth_filename = auth_filename.format(WORKSPACE=os.environ['WORKSPACE'])
        if os.path.exists(auth_filename):
            with open(auth_filename) as auth_file:
                tokens.update(json.load(auth_file))

    # update "known" environments with the values from the auth file.
    # this would not be necessary if the configuration file had all the values

    field_map = {
        'sql_password': 'rds_pass',
        'mongo_user': 'mongo_user',
        'mongo_password': 'mongo_pass',
        'secret_key': 'secret_key'
    }

    for env in ['prod', 'edge']:
        if env in environments:
            data = environments.get(env, {})
            for config_name, token_name in six.iteritems(field_map):
                data[config_name] = tokens.get(token_name)

            # different settings for edge
            if env == 'edge':
                data['sql_password'] = tokens.get('rds_pass_edge')
                data['mongo_user'] = tokens.get('mongo_user_edge')
                data['mongo_password'] = tokens.get('mongo_pass_edge')

            environments[env] = data

    config['environments'] = environments


def update_organizations(config):
    values = config['values']

    # lowercase orgs before selection
    organizations = {org.lower(): values for org, values
                     in six.iteritems(config['organizations'])}

    # select only organizations in arguments
    organizations = filter_keys(organizations, values.get('org'))

    config['organizations'] = organizations


def update_tasks(config):
    values = config['values']
    tasks = values.get('task', []) or values.get('tasks', [])

    if 'task' in values:
        del values['task']

    if tasks:
        values['tasks'] = tasks


def get_config_for_org(config, organization):
    org_config = merge(config['organizations'][organization], config['values'])
    org_config['organization'] = organization
    org_config['environments'] = config['environments']
    return org_config


def get_config_for_course(config, course):
    # config['values'] are overridden default values with program options, every other key is from the config file.
    course_config = merge(config['values'], {'tasks': config['tasks']})
    course_config['course'] = course
    course_config['environments'] = config['environments']
    return course_config


def get_config_for_env(config, environment):
    env_config = merge(config, config['environments'][environment])
    env_config['environment'] = environment
    return env_config


def setup_logging():
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            }
        },
        'handlers': {
            'default': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': 'INFO',
                'propagate': True
            }
        }
    })
