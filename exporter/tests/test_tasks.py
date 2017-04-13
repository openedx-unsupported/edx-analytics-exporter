from copy import deepcopy

from exporter import tasks

import mock


@mock.patch('exporter.tasks.execute_shell')
def test_org_email_opt_in_task(mock_execute_shell):
    """ The email_opt_in_list django admin command should be invoked with multiple organizations if the config
    includes a non-empty `other_names` section. """
    kwargs = {
        'organization': 'the-org',
        'other_names': ['the-second-org', 'the-third-org'],
        'courses': ['course-1', 'course-2'],
        'django_config': 'the-django-config',
        'django_user': 'the-django-user',
        'django_admin': 'the-django-admin',
        'django_settings': 'the-django-setings',
        'django_pythonpath': 'the-django-python-path',
    }
    filename = 'the-filename'
    dry_run = False

    command = tasks.OrgEmailOptInTask.run(filename, dry_run, **kwargs)

    assert command.endswith('the-filename the-org the-second-org the-third-org --courses=course-1,course-2')
    assert 'email_opt_in_list' in command
    expected_kwargs = deepcopy(kwargs)
    expected_kwargs['all_organizations'] = 'the-org the-second-org the-third-org'
    expected_kwargs['comma_sep_courses'] = 'course-1,course-2'
    mock_execute_shell.assert_called_once_with(command, **expected_kwargs)
