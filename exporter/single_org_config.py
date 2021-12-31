# pylint: disable=missing-docstring

import logging
import logging.config
import tempfile

from docopt import docopt

from exporter.util import merge, filter_keys

log = logging.getLogger(__name__)


def setup(doc, argv=None):
    """
    Setup export script with
    config data provided as cli options
    """
    program_options = docopt(doc, argv=argv)
    setup_logging()

    log.info('Reading configuration')

    return _get_config(program_options)


def _get_config(program_options):
    config = {}
    
    merge_program_options(config, program_options)

    update_exclude_tasks(config)

    set_config_defaults(config)

    return config


def set_config_defaults(config):
    """
    Set common default values if
    not already set using cli options
    """
    values = config['values']

    if not values.get('lms_config'):
        values['lms_config'] = '/edx/etc/lms.yml'

    if not values.get('studio_config'):
        values['studio_config'] = '/edx/etc/studio.yml'

    if not values.get('django_admin'):
        values['django_admin'] = 'django-admin'

    if not values.get('django_pythonpath'):
        values['django_pythonpath'] = '/edx/app/edxapp/edx-platform'

    values['name'] = values['environment']

    if not values.get('output_prefix'):
        values['output_prefix'] = values['organization']

    values['exclude_tasks'] += [
            'AssessmentCriterionTask', 
            'AssessmentCriterionOptionTask', 
            'AssessmentRubricTask', 
            'AssessmentTrainingExampleTask', 
            'AssessmentTrainingExampleOptionsSelectedTask'
        ]


def merge_program_options(config, program_options):
    """
    Get all the configs from CLI options
    """
    # get program options, removing '--' and replacing '-' with '_'
    options = {k[2:].replace('-', '_'): v for k, v
               in program_options.items()
               if k.startswith('--')}

    if not options.get('work_dir'):
        options['work_dir'] = tempfile.gettempdir()

    config['values'] = options

def update_exclude_tasks(config):
    values = config['values']

    exclude_tasks = values.get('exclude_task', [])

    if 'exclude_task' in values:
        del values['exclude_task']

    values['exclude_tasks'] = exclude_tasks


def get_config_for_course(config, course):
    course_config = merge(config['values'], {})
    course_config['course'] = course
    return course_config


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
