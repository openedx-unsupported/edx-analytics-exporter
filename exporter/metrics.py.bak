import calendar
from contextlib import contextmanager
from datetime import datetime
import logging
import time

import graphitesend

from exporter.config import setup_logging


setup_logging()

log = logging.getLogger(__name__)


NO_COURSE = 'NO_COURSE'

SEPARATOR = '.'


def timestamp(dt):
    return calendar.timegm(dt.utctimetuple())


def get_graphite_kwargs(kwargs):
    graphite_keys = ('graphite_host', 'graphite_port', 'graphite_prefix', 'system_name')
    return {key: kwargs[key] for key in graphite_keys if key in kwargs}


def get_graphite_client(graphite_host=None, graphite_port=None, graphite_prefix=None, system_name=''):
    if graphite_host and graphite_port and graphite_prefix:
        return graphitesend.init(
            graphite_server=graphite_host,
            graphite_port=graphite_port,
            prefix=graphite_prefix,
            system_name=system_name,
        )


def get_metric_name(task_cls, **kwargs):
    if kwargs.get('organization'):
        return SEPARATOR.join((
            kwargs['organization'],
            kwargs.get('course', NO_COURSE).replace(SEPARATOR, '_'),
            task_cls.__name__
        ))

@contextmanager
def collect_elapsed_time(cls, **kwargs):
    start_time = time.time()
    yield
    elapsed_time = time.time() - start_time

    metric_name = get_metric_name(cls, **kwargs)
    if not metric_name:
        log.info('No organization for this task, not sending metrics.')
        return


    metric_name = SEPARATOR.join((metric_name, 'elapsed_time'))

    try:
        # the kwargs passed to a Task.run() method include configuration options that point at a graphite server
        client = get_graphite_client(**get_graphite_kwargs(kwargs))
        if client:
            log.info('Logging elapsed time for %s to graphite.', metric_name)
            client.send(metric_name, elapsed_time, timestamp(datetime.utcnow()))
            client.disconnect()
        else:
            log.warn('No graphite client!: %s', get_graphite_kwargs(kwargs))
    except graphitesend.GraphiteSendException as exc:
        log.warn('Graphite send exception for metric %s: %s', metric_name, str(exc))
