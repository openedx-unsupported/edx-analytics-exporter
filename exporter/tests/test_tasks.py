# -*- coding: utf-8 -*

import os
from copy import deepcopy

from exporter import tasks

import mock
import pytest


class TestOrgTask(tasks.OrgTask, tasks.Task):
    NAME = 'test_org_task'
    EXT = 'csv'


class TestCourseTask(tasks.CourseTask, tasks.Task):
    NAME = 'test_course_task'
    EXT = 'csv'


@mock.patch('exporter.tasks.execute_shell')
def test_org_email_opt_in_task(mock_execute_shell):
    """ The email_opt_in_list django admin command should be invoked with multiple organizations if the config
    includes a non-empty `other_names` section. """
    kwargs = {
        'organization': 'the-org',
        'other_names': ['the-second-org', 'the-third-org'],
        'courses': ['course-1', u'coursé-2'],
        'lms_config': 'the-lms-config',
        'studio_config': 'the-studio-config',
        'django_admin': 'the-django-admin',
        'django_settings': 'the-django-setings',
        'django_pythonpath': 'the-django-python-path',
    }
    filename = 'the-filename'
    dry_run = False

    command = tasks.OrgEmailOptInTask.run(filename, dry_run, **kwargs)

    assert command.endswith(u'the-filename the-org the-second-org the-third-org --courses=course-1,coursé-2 --email-optin-chunk-size=10000')
    assert 'email_opt_in_list' in command
    expected_kwargs = deepcopy(kwargs)
    expected_kwargs['all_organizations'] = 'the-org the-second-org the-third-org'
    expected_kwargs['comma_sep_courses'] = u'course-1,coursé-2'
    expected_kwargs['max_tries'] = 3
    mock_execute_shell.assert_called_once_with(command, **expected_kwargs)


def test_get_filename_org_task():
    kwargs = {
        'name': 'test-analytics',
        'work_dir': '/tmp/workdir/',
        'organization': 'testx',
    }
    assert '/tmp/workdir/testx-test_org_task-test-analytics.csv' == TestOrgTask.get_filename(**kwargs)


def test_get_filename_course_task():
    kwargs = {
        'name': 'test-analytics',
        'work_dir': '/tmp/workdir/',
        'course': 'course-v1:edX+DemoX+Demo_Course',
    }
    assert '/tmp/workdir/edX-DemoX-Demo_Course-test_course_task-test-analytics.csv' == TestCourseTask.get_filename(**kwargs)


def test_get_non_ascii_filename_course_task():
    kwargs = {
        'name': 'test-analytics',
        'work_dir': '/tmp/workdir/',
        'course': u'course-v1:edX+DemoX+Démo_Course',
    }
    assert '/tmp/workdir/edX-DemoX-D_mo_Course-test_course_task-test-analytics.csv' == TestCourseTask.get_filename(**kwargs)

@pytest.mark.parametrize(
    'name',
    [
        'a',
        'a' * (255 - 1 - len(TestCourseTask.EXT)),
        'a' * (256 - 1 - len(TestCourseTask.EXT)),
        'a' * (1000 - 1 - len(TestCourseTask.EXT)),
    ]
)
def test_course_task_get_filename_on_multiple_sizes(name):
    """
    Test TestCourseTask.get_filename with multiple name sizes
    """
    kwargs = {
        'name': name,
        'work_dir': '/tmp/workdir/',
        'course': 'course-v1:edX+DemoX+Demo_Course',
    }
    with mock.patch('exporter.tasks._get_max_filename_length', return_value=255):
        file_name = os.path.basename(TestCourseTask.get_filename(**kwargs))
        # The base name without an extension should have a length that is
        # less than or equal to 255 - 20
        assert len(os.path.splitext(file_name)[0]) <= 235


def test_course_task_get_filename_on_similar_names():
    """
    Test that 3 very long names with a one character difference don't end up
    having duplicate file names.
    """
    kwargs = {
        'work_dir': '/tmp/workdir/',
        'course': 'course-v1:edX+DemoX+Demo_Course',
    }
    with mock.patch('exporter.tasks._get_max_filename_length', return_value=255):
        kwargs['name'] = 'a' * 1000
        file_name1 = os.path.basename(TestCourseTask.get_filename(**kwargs))
        # Change the leading 'a' to 'b'
        kwargs['name'] = ('a' * 1000).replace('a', 'b', 1)
        file_name2 = os.path.basename(TestCourseTask.get_filename(**kwargs))
        # Change the trailing 'a' to 'b'
        kwargs['name'] = ('a' * 999) + 'b'
        file_name3 = os.path.basename(TestCourseTask.get_filename(**kwargs))
        assert len(set([file_name1, file_name2, file_name3])) == 3
