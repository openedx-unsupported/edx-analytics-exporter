from datetime import datetime
import time

import graphitesend
import mock

from exporter.config import setup_logging
from exporter.metrics import collect_elapsed_time, get_graphite_client, timestamp
from exporter.tasks import Task


setup_logging()


class MockDateTime(datetime):
    NOW = datetime(2017, 1, 1)

    @classmethod
    def utcnow(cls):
        return cls.NOW


class MockGraphiteClient(object):
    def __init__(self, *args, **kwargs):
        self.metrics = {}

    def send(self, name, value, emission_timestamp):
        self.metrics[(name, emission_timestamp)] = value

    def disconnect(self):
        pass


class MockFailingGraphiteClient(MockGraphiteClient):
    def send(self, name, value, emission_timestamp):
        raise graphitesend.GraphiteSendException('the error')


class MockTask(Task):
    @classmethod
    def run(cls, filename, dry_run, **kwargs):
        super(MockTask, cls).run(filename, dry_run, **kwargs)

        # pretend we do some work
        time.sleep(0.1)

        return 'the-result'


def test_get_graphite_client():
    """The get_graphite_client() function should pass along relevant graphite arguments to graphitesend.init()."""
    with mock.patch('exporter.metrics.graphitesend.init') as graphite_init:

        kwargs = {
            'graphite_host': 'the-host',
            'graphite_port': 'the-port',
            'graphite_prefix': 'the-prefix',
        }
        the_client = get_graphite_client(**kwargs)

        assert graphite_init.return_value == the_client
        graphite_init.assert_called_once_with(
            graphite_server='the-host',
            graphite_port='the-port',
            prefix='the-prefix',
            system_name=''
        )


def test_collect_elapsed_time_happy_path():
    """Decorating a Task's run method with @collect_elapsed_time should store some metric via the MetricClient."""
    mock_client = MockGraphiteClient()
    with mock.patch('exporter.metrics.get_graphite_client', return_value=mock_client) as mock_get_client, \
         mock.patch('exporter.metrics.datetime', MockDateTime):

        kwargs = {
            'graphite_host': 'the-host',
            'graphite_port': 'the-port',
            'graphite_prefix': 'the-prefix',
            'other_option': 'who-cares',
            'organization': 'edX',
            'course': 'the-course'
        }
        with collect_elapsed_time(MockTask, **kwargs):
            result = MockTask.run('my.file', True, **kwargs)

        assert 'the-result' == result
        emission_timestamp = timestamp(MockDateTime.NOW)
        assert 0.1 <= mock_client.metrics[('edX.the-course.MockTask.elapsed_time', emission_timestamp)] < 0.2
        mock_get_client.assert_called_once_with(
            graphite_host='the-host',
            graphite_port='the-port',
            graphite_prefix='the-prefix',
        )


def test_collect_elapsed_time_no_org():
    """When no organization exists in kwargs, the collect_elapsed_time decorator should return without
    sending anything to graphite."""
    mock_client = MockGraphiteClient()
    with mock.patch('exporter.metrics.get_graphite_client', return_value=mock_client) as mock_get_client, \
         mock.patch('exporter.metrics.datetime', MockDateTime):

        kwargs = {
            'graphite_host': 'the-host',
            'graphite_port': 'the-port',
            'graphite_prefix': 'the-prefix',
            'other_option': 'who-cares',
            'course': 'the-course'
        }
        with collect_elapsed_time(MockTask, **kwargs):
            result = MockTask.run('my.file', True, **kwargs)

        assert 'the-result' == result
        assert not mock_get_client.called
        assert {} == mock_client.metrics


def test_graphite_send_exception():
    """When a GraphiteSendException occurs, @collect_elapsed_time should not cause the run() method to fail."""
    mock_client = MockFailingGraphiteClient()
    with mock.patch('exporter.metrics.get_graphite_client', return_value=mock_client) as mock_get_client, \
         mock.patch('exporter.metrics.datetime', MockDateTime):

        kwargs = {
            'graphite_host': 'the-host',
            'graphite_port': 'the-port',
            'graphite_prefix': 'the-prefix',
            'other_option': 'who-cares',
            'organization': 'edX',
            'course': 'the-course',
        }
        with collect_elapsed_time(MockTask, **kwargs):
            result = MockTask.run('my.file', True, **kwargs)

        assert 'the-result' == result
        assert {} == mock_client.metrics
        mock_get_client.assert_called_once_with(
            graphite_host='the-host',
            graphite_port='the-port',
            graphite_prefix='the-prefix',
        )


def test_no_graphite_client_configured():
    """If the graphite configuration information is not present, a Task's run() method should still run."""
    mock_client = MockFailingGraphiteClient()
    with mock.patch('exporter.metrics.get_graphite_client', return_value=mock_client) as mock_get_client, \
         mock.patch('exporter.metrics.datetime', MockDateTime):

        kwargs = {
            'other_option': 'who-cares',
            'organization': 'edX',
            'course': 'the-course',
        }
        with collect_elapsed_time(MockTask, **kwargs):
            result = MockTask.run('my.file', True, **kwargs)

        assert 'the-result' == result
        assert {} == mock_client.metrics
        mock_get_client.assert_called_once_with()
