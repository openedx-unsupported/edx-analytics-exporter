# -*- coding: utf-8 -*

from copy import deepcopy

from exporter import tasks

import mock


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
        'django_config': 'the-django-config',
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
