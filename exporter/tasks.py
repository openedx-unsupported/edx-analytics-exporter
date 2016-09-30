# pylint: disable=missing-docstring

import logging
import os
import subprocess
import distutils

from opaque_keys.edx.keys import CourseKey

from exporter.util import NotSet, execute_shell
from exporter.mysql_query import MysqlDumpQueryToTSV


log = logging.getLogger(__name__)

MAX_TRIES_FOR_MARKER_FILE_CHECK = 5
MAX_TRIES_FOR_COPY_FILE_FROM_S3 = 5


class FatalTaskError(Exception):
    """Exception marking tasks that should be treated as fatal."""
    pass


class Task(object):
    """ Base class for all Task. """
    NAME = NotSet
    EXT = NotSet

    @classmethod
    def run(cls, filename, dry_run, **kwargs):
        pass


class OrgTask(object):
    """ Mixin class for organization level tasks."""

    @classmethod
    def get_filename(cls, **kwargs):
        template = "{org}-{task}-{name}.{extension}"

        filename = template.format(
            org=kwargs['organization'],
            task=cls.NAME,
            name=kwargs['name'],
            extension=cls.EXT
        )
        return os.path.join(kwargs['work_dir'], filename)


class CourseTask(object):
    """ Mixin class for course level tasks."""

    SUBDIR = NotSet

    @classmethod
    def get_filename(cls, **kwargs):
        template = "{course}-{task}-{name}.{extension}"

        filename = template.format(
            course=cls.get_course_name(kwargs['course']),
            task=cls.NAME,
            environment=kwargs['environment'],
            name=kwargs['name'],
            extension=cls.EXT
        )
        if cls.SUBDIR != NotSet:
            return os.path.join(kwargs['work_dir'], cls.SUBDIR, filename)
        else:
            return os.path.join(kwargs['work_dir'], filename)

    @classmethod
    def get_course_name(cls, course_id):
        course_key = CourseKey.from_string(course_id)
        if hasattr(course_key, 'ccx'):
            course = '-'.join((course_key.org, course_key.course, course_key.run, 'ccx', course_key.ccx))
        else:
            course = '-'.join((course_key.org, course_key.course, course_key.run))

        return course


def clean_command(command):
    return ' '.join(l.strip() for l in command.split('\n')).strip()


class SQLTask(Task):
    NAME = NotSet
    SQL = NotSet
    EXT = 'sql'

    @classmethod
    def run(cls, filename, dry_run, **kwargs):
        super(SQLTask, cls).run(filename, dry_run, **kwargs)

        query = cls.get_query(**kwargs)

        log.debug(query)

        if dry_run:
            print 'SQL: {0}'.format(query)
        else:
            mysql_query = MysqlDumpQueryToTSV(kwargs.get('sql_host'), kwargs.get('sql_user'), kwargs.get('sql_password'), kwargs.get('sql_db'), filename)
            mysql_query.execute(query)

    @classmethod
    def get_query(cls, **kwargs):
        sql = clean_command(cls.SQL)

        if kwargs.get('limit'):
            sql = '{0} limit {1}'.format(sql, kwargs['limit'])

        values = cls.get_values(**kwargs)
        query = '{0};'.format(sql.format(**values))

        return query

    @classmethod
    def get_values(cls, **kwargs):
        return kwargs


class MongoTask(Task):
    NAME = NotSet
    QUERY = NotSet
    EXT = 'mongo'
    CMD = """
    mongoexport
      --host {mongo_host}
      --db {mongo_db}
      --username {mongo_user}
      --password "{mongo_password}"
      --collection {mongo_collection}
      --query '{query}'
      --slaveOk
      --out {filename}
      >&2
    """

    @classmethod
    def run(cls, filename, dry_run, **kwargs):
        super(MongoTask, cls).run(filename, dry_run, **kwargs)

        query = clean_command(cls.QUERY).format(**kwargs)

        log.debug(query)

        cmd = clean_command(cls.CMD)
        cmd = cmd.format(filename=filename, query=query, **kwargs)

        if dry_run:
            print 'MONGO: {0}'.format(query)
        else:
            execute_shell(cmd, **kwargs)


class DjangoAdminTask(Task):
    NAME = NotSet
    COMMAND = NotSet
    ARGS = NotSet
    EXT = NotSet
    VARS = 'CONFIG_ROOT={django_config} SERVICE_VARIANT=lms'
    OUT = '/dev/null'
    CMD = """
    sudo -E -u {django_user} {variables}
      {django_admin} {command}
      --settings={django_settings}
      --pythonpath={django_pythonpath}
      {arguments}
    > {output}
    """

    @classmethod
    def run(cls, filename, dry_run, **kwargs):
        super(DjangoAdminTask, cls).run(filename, dry_run, **kwargs)

        command = cls.COMMAND
        arguments = cls.ARGS.format(filename=filename, **kwargs)
        output = cls.OUT.format(filename=filename, **kwargs)
        variables = cls.VARS.format(**kwargs)

        # --database={django_database}
        # if 'django_database' not in kwargs:
        #     kwargs['django_database'] = 'default'

        cmd = clean_command(cls.CMD)
        cmd = cmd.format(
            command=command,
            output=output,
            arguments=arguments,
            variables=variables,
            **kwargs)

        log.info("Running django command %s.", cmd)

        if dry_run:
            print cmd
        else:
            execute_shell(cmd, **kwargs)


class CopyS3FileTask(Task):
    NAME = NotSet
    EXT = NotSet

    @classmethod
    def run(cls, filename, dry_run, **kwargs):
        super(CopyS3FileTask, cls).run(filename, dry_run, **kwargs)

        if not distutils.spawn.find_executable("aws"):
            raise FatalTaskError("The {0} task requires the awscli".format(cls.__name__))

        file_basename = os.path.basename(filename)
        s3_source_filename = '{prefix}/{env}/{filename}'.format(
            prefix=kwargs['external_prefix'],
            env=kwargs['environment'],
            filename=file_basename
        )
        s3_marker_filename = '{prefix}/{env}/{filename}'.format(
            prefix=kwargs['external_prefix'],
            env=kwargs['environment'],
            filename='job_success/_SUCCESS'
        )

        if dry_run:
            print 'Copy S3 File: {0} to {1}'.format(
                s3_source_filename,
                filename)
        else:
            # First check to see that the export data was successfully generated
            # by looking for a marker file for that run. Return a more severe failure,
            # so that the overall environment dump fails, rather than just the particular
            # file being copied.

            head_command = "aws s3api head-object --bucket {bucket} --key {key}"

            marker_command = head_command.format(
                bucket=kwargs['pipeline_bucket'],
                key=s3_marker_filename
            )

            source_command = head_command.format(
                bucket=kwargs['pipeline_bucket'],
                key=s3_source_filename
            )

            try:
                log.info("Running command with retries: %s.", marker_command)
                # Define retries here, to recover from temporary outages when calling S3 to find files.
                local_kwargs = dict(**kwargs)
                local_kwargs['max_tries'] = MAX_TRIES_FOR_MARKER_FILE_CHECK
                execute_shell(marker_command, **local_kwargs)
            except subprocess.CalledProcessError:
                error_message = 'Unable to find success marker for export {0}'.format(s3_marker_filename)
                log.error(error_message)
                raise FatalTaskError(error_message)

            # Then check that the source file exists.  It's okay if it isn't,
            # as that will happen when a particular database table is empty.
            try:
                log.info("Running command %s.", source_command)
                execute_shell(source_command, **kwargs)
            except subprocess.CalledProcessError:
                log.info('Unable to find %s to copy.', s3_source_filename)
            else:
                try:
                    cmd = 'aws s3 cp s3://{bucket}/{src} {dest}'.format(
                        bucket=kwargs['pipeline_bucket'],
                        src=s3_source_filename,
                        dest=filename
                    )
                    # Define retries here, to recover from temporary outages when calling S3 to copy files.
                    local_kwargs = dict(**kwargs)
                    local_kwargs['max_tries'] = MAX_TRIES_FOR_COPY_FILE_FROM_S3
                    execute_shell(cmd, **local_kwargs)
                except subprocess.CalledProcessError:
                    log.error('Unable to copy %s to %s', s3_source_filename, filename)
                    raise


class UserIDMapTask(CourseTask, SQLTask):
    NAME = 'user_id_map'
    SQL = """
    SELECT CAST(md5(concat('{secret_key}', au0.id)) AS CHAR) hash_id,
           au0.id,
           au0.username
    FROM {sql_db}.auth_user au0
    WHERE au0.id IN
        (SELECT DISTINCT(auth_user.id) USER_ID
         FROM {sql_db}.auth_user
         INNER JOIN student_courseenrollment ON {sql_db}.student_courseenrollment.USER_ID = auth_user.id
         WHERE course_id='{course}')
    """


class StudentModuleTask(CourseTask, CopyS3FileTask):
    NAME = 'courseware_studentmodule'
    EXT = 'sql'


class TeamsTask(CourseTask, SQLTask):
    NAME = 'teams'
    SQL = """
    SELECT *
    FROM teams_courseteam
    WHERE teams_courseteam.course_id='{course}'
    """


class TeamsMembershipTask(CourseTask, SQLTask):
    NAME = 'teams_membership'
    SQL = """
    SELECT teams_courseteammembership.*
    FROM teams_courseteam
    INNER JOIN teams_courseteammembership
    ON teams_courseteam.id=teams_courseteammembership.team_id
    WHERE teams_courseteam.course_id='{course}'
    """


class CourseEnrollmentTask(CourseTask, SQLTask):
    NAME = 'student_courseenrollment'
    SQL = """
    SELECT *
    FROM student_courseenrollment
    WHERE course_id='{course}'
    """


class GeneratedCertificateTask(CourseTask, SQLTask):
    NAME = 'certificates_generatedcertificate'
    SQL = """
    SELECT *
    FROM certificates_generatedcertificate
    WHERE course_id='{course}'
    """


class InCourseReverificationTask(CourseTask, SQLTask):
    NAME = 'verify_student_verificationstatus'
    SQL = """
    SELECT vs.timestamp,
           vs.status,
           vc.course_id,
           vc.checkpoint_location,
           vs.user_id
    FROM verify_student_verificationstatus AS vs
    LEFT JOIN verify_student_verificationcheckpoint AS vc ON vs.checkpoint_id=vc.id
    WHERE vc.course_id='{course}'
    ORDER BY vs.timestamp ASC
    """


class AuthUserTask(CourseTask, SQLTask):
    NAME = 'auth_user'
    SQL = """
    SELECT auth_user.id,
           auth_user.username,
           auth_user.first_name,
           auth_user.last_name,
           auth_user.email,
           '' AS password,
           auth_user.is_staff,
           auth_user.is_active,
           auth_user.is_superuser,
           auth_user.last_login,
           auth_user.date_joined,
           '' AS status,
           NULL AS email_key,
           '' AS avatar_type,
           '' AS country,
           0 AS show_country,
           NULL AS date_of_birth,
           '' AS interesting_tags,
           '' AS ignored_tags,
           0 AS email_tag_filter_strategy,
           0 AS display_tag_filter_strategy,
           0 AS consecutive_days_visit_count
    FROM auth_user
    INNER JOIN student_courseenrollment ON student_courseenrollment.user_id = auth_user.id
    AND student_courseenrollment.course_id = '{course}'
    """


class AuthUserProfileTask(CourseTask, SQLTask):
    NAME = 'auth_userprofile'
    SQL = """
    SELECT auth_userprofile.*
    FROM auth_userprofile
    INNER JOIN student_courseenrollment ON student_courseenrollment.user_id = auth_userprofile.user_id
    AND student_courseenrollment.course_id = '{course}'
    """


class StudentLanguageProficiencyTask(CourseTask, SQLTask):
    NAME = 'student_languageproficiency'
    SQL = """
    SELECT student_languageproficiency.*
    FROM student_languageproficiency
    INNER JOIN auth_userprofile ON auth_userprofile.id = student_languageproficiency.user_profile_id
    INNER JOIN student_courseenrollment ON student_courseenrollment.user_id = auth_userprofile.user_id
    AND student_courseenrollment.course_id = '{course}'
    """


class CourseWikiTask(CourseTask):
    """ Mixin for Course Wiki related tasks """
    @classmethod
    def run(cls, filename, dry_run, **kwargs):
        course_key = CourseKey.from_string(kwargs['course'])
        kwargs['slug'] = course_key.course
        super(CourseWikiTask, cls).run(filename, dry_run, **kwargs)


class WikiArticleTask(CourseWikiTask, SQLTask):
    NAME = 'wiki_article'
    SQL = """
    SELECT a.*
    FROM {sql_db}.wiki_article AS a
    WHERE a.id IN
        (SELECT node.id
         FROM {sql_db}.wiki_urlpath AS node,
              {sql_db}.wiki_urlpath AS parent
         WHERE node.lft BETWEEN parent.lft AND parent.rght
           AND parent.slug = '{slug}'
         ORDER BY node.lft)
    """


class WikiArticleRevisionTask(CourseWikiTask, SQLTask):
    NAME = 'wiki_articlerevision'
    SQL = """
    SELECT ar.*
    FROM {sql_db}.wiki_articlerevision AS ar
    WHERE ar.article_id IN
        (SELECT a.id
         FROM {sql_db}.wiki_article AS a
         WHERE a.id IN
             (SELECT node.id
              FROM {sql_db}.wiki_urlpath AS node,
                   {sql_db}.wiki_urlpath AS parent
              WHERE node.lft BETWEEN parent.lft AND parent.rght
                AND parent.slug = '{slug}'
              ORDER BY node.lft))
    ORDER BY article_id,
             revision_number
    """


class UserCourseTagTask(CourseTask, SQLTask):
    NAME = 'user_api_usercoursetag'
    SQL = """
    SELECT *
    FROM user_api_usercoursetag
    WHERE course_id='{course}'
    """


class StudentAnonymousUserIDTask(CourseTask, SQLTask):
    NAME = 'student_anonymoususerid'
    SQL = """
    SELECT * FROM student_anonymoususerid
    WHERE course_id="{course}"
    """

# Start ORA2 Tables ==================

class ORA2CourseTask(CourseTask):
    SUBDIR = "ora"


class AssessmentAIClassifierTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_aiclassifier'
    SQL = """
    SELECT * FROM `assessment_aiclassifier`
    WHERE classifier_set_id IN (SELECT id FROM assessment_aiclassifierset
                                  WHERE course_id="{course}")
    """

class AssessmentAIClassifierSetTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_aiclassifierset'
    SQL = """
    SELECT * FROM assessment_aiclassifierset
    WHERE course_id="{course}"
    """

# Not used
class AssessmentAIGradingWorkflowTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_aigradingworkflow'
    SQL = """
    SELECT * FROM assessment_aigradingworkflow
    WHERE course_id="{course}"
    """

class AssessmentAITrainingWorkflowTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_aitrainingworkflow'
    SQL = """
    SELECT * FROM assessment_aitrainingworkflow
    WHERE course_id="{course}"
    """

class AssessmentAITrainingWorkflowTrainingExamplesTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_aitrainingworkflow_training_examples'
    SQL = """
    SELECT * FROM assessment_aitrainingworkflow_training_examples AS ate
    WHERE ate.aitrainingworkflow_id IN (SELECT id FROM assessment_aitrainingworkflow
                                      WHERE course_id="{course}")
    """

class AssessmentAssessmentTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_assessment'
    SQL = """
    SELECT a.* FROM assessment_assessment AS a
    LEFT JOIN submissions_submission AS s ON a.submission_uuid=s.uuid
    LEFT JOIN submissions_studentitem AS si ON s.student_item_id=si.id
    WHERE si.course_id="{course}"
    """

class AssessmentAssessmentFeedbackTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_assessmentfeedback'
    SQL = """
    SELECT DISTINCT af.* FROM assessment_assessmentfeedback AS af
    LEFT JOIN assessment_assessmentfeedback_assessments AS afa
           ON af.id=afa.assessmentfeedback_id
    LEFT JOIN assessment_assessment AS a ON afa.assessment_id=a.id
    LEFT JOIN submissions_submission AS s ON a.submission_uuid=s.uuid
    LEFT JOIN submissions_studentitem AS si ON s.student_item_id=si.id
    WHERE si.course_id="{course}"
    """

class AssessmentAssessmentFeedbackAssessmentsTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_assessmentfeedback_assessments'
    SQL = """
    SELECT afa.* FROM assessment_assessmentfeedback_assessments AS afa
    LEFT JOIN assessment_assessment AS a ON afa.assessment_id=a.id
    LEFT JOIN submissions_submission AS s ON a.submission_uuid=s.uuid
    LEFT JOIN submissions_studentitem AS si ON s.student_item_id=si.id
    WHERE si.course_id="{course}"
    """

class AssessmentAssessmentFeedbackOptionsTask(ORA2CourseTask, SQLTask):
    """
    Note the 's' in FeedbackOptions (as compared to below)
    """
    NAME = 'assessment_assessmentfeedback_options'
    SQL = """
    SELECT DISTINCT afo.* FROM assessment_assessmentfeedback_options AS afo
    LEFT JOIN assessment_assessmentfeedback AS af ON afo.assessmentfeedback_id=af.id
    LEFT JOIN assessment_assessmentfeedback_assessments AS afa
           ON af.id=afa.assessmentfeedback_id
    LEFT JOIN assessment_assessment AS a ON afa.assessment_id=a.id
    LEFT JOIN submissions_submission AS s ON a.submission_uuid=s.uuid
    LEFT JOIN submissions_studentitem AS si ON s.student_item_id=si.id
    WHERE si.course_id="{course}"
    """

class AssessmentAssessmentFeedbackOptionTask(ORA2CourseTask, SQLTask):
    """
    Note the lack of 's' in FeedbackOption (as compared to above)
    """
    NAME = 'assessment_assessmentfeedbackoption'
    SQL = """
    SELECT DISTINCT aafo.* FROM assessment_assessmentfeedbackoption as aafo
    LEFT JOIN assessment_assessmentfeedback_options AS afo
           ON aafo.id=afo.assessmentfeedbackoption_id
    LEFT JOIN assessment_assessmentfeedback AS af ON afo.assessmentfeedback_id=af.id
    LEFT JOIN assessment_assessmentfeedback_assessments AS afa
           ON af.id=afa.assessmentfeedback_id
    LEFT JOIN assessment_assessment AS a ON afa.assessment_id=a.id
    LEFT JOIN submissions_submission AS s ON a.submission_uuid=s.uuid
    LEFT JOIN submissions_studentitem AS si ON s.student_item_id=si.id
    WHERE si.course_id="{course}"
    """

class AssessmentAssessmentPartTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_assessmentpart'
    SQL = """
    SELECT ap.* FROM assessment_assessmentpart AS ap
    LEFT JOIN assessment_assessment AS a ON ap.assessment_id=a.id
    LEFT JOIN submissions_submission AS s ON a.submission_uuid=s.uuid
    LEFT JOIN submissions_studentitem AS si ON s.student_item_id=si.id
    WHERE si.course_id="{course}"
    """

class AssessmentCriterionTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_criterion'
    SQL = """
    SELECT c.* FROM assessment_criterion AS c
    WHERE c.rubric_id IN (
        SELECT DISTINCT rub.id FROM assessment_rubric AS rub
            LEFT JOIN assessment_assessment AS a ON rub.id=a.rubric_id
            LEFT JOIN submissions_submission AS s ON a.submission_uuid=s.uuid
            LEFT JOIN submissions_studentitem AS si ON s.student_item_id=si.id
            WHERE si.course_id="{course}"
        UNION
        SELECT DISTINCT rub.id FROM assessment_rubric AS rub
            LEFT JOIN assessment_trainingexample AS te ON rub.id=te.rubric_id
            LEFT JOIN assessment_aitrainingworkflow_training_examples AS ate
                   ON te.id=ate.trainingexample_id
            LEFT JOIN assessment_aitrainingworkflow AS tw
                   ON ate.aitrainingworkflow_id=tw.id
            WHERE tw.course_id="{course}"
        UNION
        SELECT DISTINCT rub.id FROM assessment_rubric AS rub
            LEFT JOIN assessment_aigradingworkflow AS aigw ON rub.id=aigw.rubric_id
            WHERE aigw.course_id="{course}"
        UNION
        SELECT DISTINCT rub.id FROM assessment_rubric AS rub
            LEFT JOIN assessment_aiclassifierset AS acs ON rub.id=acs.rubric_id
            WHERE acs.course_id="{course}")
    """

class AssessmentCriterionOptionTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_criterionoption'
    SQL = """
    SELECT co.* FROM assessment_criterionoption AS co
    WHERE co.criterion_id IN (
        SELECT c.id FROM assessment_criterion AS c
        WHERE c.rubric_id IN (
            SELECT DISTINCT rub.id FROM assessment_rubric AS rub
                LEFT JOIN assessment_assessment AS a ON rub.id=a.rubric_id
                LEFT JOIN submissions_submission AS s ON a.submission_uuid=s.uuid
                LEFT JOIN submissions_studentitem AS si ON s.student_item_id=si.id
                WHERE si.course_id="{course}"
            UNION
            SELECT DISTINCT rub.id FROM assessment_rubric AS rub
                LEFT JOIN assessment_trainingexample AS te ON rub.id=te.rubric_id
                LEFT JOIN assessment_aitrainingworkflow_training_examples AS ate
                       ON te.id=ate.trainingexample_id
                LEFT JOIN assessment_aitrainingworkflow AS tw
                       ON ate.aitrainingworkflow_id=tw.id
                WHERE tw.course_id="{course}"
            UNION
            SELECT DISTINCT rub.id FROM assessment_rubric AS rub
                LEFT JOIN assessment_aigradingworkflow AS aigw ON rub.id=aigw.rubric_id
                WHERE aigw.course_id="{course}"
            UNION
                SELECT DISTINCT rub.id FROM assessment_rubric AS rub
                LEFT JOIN assessment_aiclassifierset AS acs ON rub.id=acs.rubric_id
                WHERE acs.course_id="{course}"))
    """

class AssessmentPeerWorkflowTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_peerworkflow'
    SQL = """
    SELECT * FROM assessment_peerworkflow
    WHERE course_id="{course}"
    """

class AssessmentPeerWorkflowItemTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_peerworkflowitem'
    SQL = """
    SELECT * FROM assessment_peerworkflowitem
    WHERE assessment_id IN (SELECT id FROM assessment_peerworkflow
                      WHERE course_id="{course}")
    """

class AssessmentRubricTask(ORA2CourseTask, SQLTask):
    """
    There can be rubrics for assessments, training examples, AI Grading Workflows,
    AIClassifierSets.  There will likely be duplicates, but just UNION them all. (Is there
    a shorter way to do this?)
    """
    NAME = 'assessment_rubric'
    SQL = """
    SELECT DISTINCT rub.* FROM assessment_rubric AS rub
        LEFT JOIN assessment_assessment AS a ON rub.id=a.rubric_id
        LEFT JOIN submissions_submission AS s ON a.submission_uuid=s.uuid
        LEFT JOIN submissions_studentitem AS si ON s.student_item_id=si.id
        WHERE si.course_id="{course}"
    UNION
    SELECT DISTINCT rub.* FROM assessment_rubric AS rub
        LEFT JOIN assessment_trainingexample AS te ON rub.id=te.rubric_id
        LEFT JOIN assessment_aitrainingworkflow_training_examples AS ate
               ON te.id=ate.trainingexample_id
        LEFT JOIN assessment_aitrainingworkflow AS tw ON ate.aitrainingworkflow_id=tw.id
        WHERE tw.course_id="{course}"
    UNION
    SELECT DISTINCT rub.* FROM assessment_rubric AS rub
        LEFT JOIN assessment_aigradingworkflow AS aigw ON rub.id=aigw.rubric_id
        WHERE aigw.course_id="{course}"
    UNION
    SELECT DISTINCT rub.* FROM assessment_rubric AS rub
        LEFT JOIN assessment_aiclassifierset AS acs ON rub.id=acs.rubric_id
        WHERE acs.course_id="{course}"
    """

class AssessmentStudentTrainingWorkflow(ORA2CourseTask, SQLTask):
    NAME = 'assessment_studenttrainingworkflow'
    SQL = """
    SELECT * FROM assessment_studenttrainingworkflow
    WHERE course_id="{course}"
    """

class AssessmentStudentTrainingWorkflowItemTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_studenttrainingworkflowitem'
    SQL = """
    SELECT * FROM assessment_studenttrainingworkflowitem
    WHERE workflow_id IN (SELECT id FROM assessment_studenttrainingworkflow
                          WHERE course_id="{course}")
    """


class AssessmentTrainingExampleTask(ORA2CourseTask, SQLTask):
    """
    This can be used from AITrainingWorkflow or StudentTrainingWOrkflowItem, so
    UNION the two.
    """
    NAME = 'assessment_trainingexample'
    SQL = """
    SELECT DISTINCT te.* FROM assessment_trainingexample AS te
        LEFT JOIN assessment_aitrainingworkflow_training_examples AS ate
               ON te.id=ate.trainingexample_id
        LEFT JOIN assessment_aitrainingworkflow AS tw ON ate.aitrainingworkflow_id=tw.id
        WHERE tw.course_id="{course}"
    UNION
    SELECT DISTINCT te.*  FROM assessment_trainingexample AS te
        LEFT JOIN assessment_studenttrainingworkflowitem AS stwi
               ON te.id=stwi.training_example_id
        LEFT JOIN assessment_studenttrainingworkflow AS stw ON stwi.workflow_id=stw.id
        WHERE stw.course_id="{course}"
    """

class AssessmentTrainingExampleOptionsSelectedTask(ORA2CourseTask, SQLTask):
    NAME = 'assessment_trainingexample_options_selected'
    SQL = """
    SELECT tos.* FROM assessment_trainingexample_options_selected AS tos
    WHERE tos.trainingexample_id IN (
        SELECT DISTINCT te.id FROM assessment_trainingexample AS te
            LEFT JOIN assessment_aitrainingworkflow_training_examples AS ate
                   ON te.id=ate.trainingexample_id
            LEFT JOIN assessment_aitrainingworkflow AS tw
                   ON ate.aitrainingworkflow_id=tw.id
            WHERE tw.course_id="{course}"
        UNION
        SELECT DISTINCT te.id  FROM assessment_trainingexample AS te
            LEFT JOIN assessment_studenttrainingworkflowitem AS stwi
                   ON te.id=stwi.training_example_id
            LEFT JOIN assessment_studenttrainingworkflow AS stw ON stwi.workflow_id=stw.id
            WHERE stw.course_id="{course}")
    """

class SubmissionsScoreTask(ORA2CourseTask, SQLTask):
    NAME = 'submissions_score'
    SQL = """
    SELECT * FROM submissions_score
    WHERE student_item_id IN (SELECT id FROM submissions_studentitem
                              WHERE course_id="{course}")
    """

class SubmissionsScoreSummaryTask(ORA2CourseTask, SQLTask):
    NAME = 'submissions_scoresummary'
    SQL = """
    SELECT * FROM submissions_scoresummary
    WHERE student_item_id IN (SELECT id FROM submissions_studentitem
                              WHERE course_id="{course}")
    """

class SubmissionsStudentItemTask(ORA2CourseTask, SQLTask):
    NAME = 'submissions_studentitem'
    SQL = """
    SELECT * FROM submissions_studentitem
    WHERE course_id="{course}"
    """

class SubmissionsSubmissionTask(ORA2CourseTask, SQLTask):
    NAME = 'submissions_submission'
    SQL = """
    SELECT * FROM submissions_submission
    WHERE student_item_id IN (SELECT id FROM submissions_studentitem
                              WHERE course_id="{course}")
    """

class WorkflowAssessmentWorkflowTask(ORA2CourseTask, SQLTask):
    NAME = 'workflow_assessmentworkflow'
    SQL = """
    SELECT * FROM workflow_assessmentworkflow
    WHERE course_id="{course}"
    """

class WorkflowAssessmentWorkflowStepTask(ORA2CourseTask, SQLTask):
    NAME = 'workflow_assessmentworkflowstep'
    SQL = """
    SELECT * FROM workflow_assessmentworkflowstep
    WHERE workflow_id IN (SELECT id FROM workflow_assessmentworkflow
                          WHERE course_id="{course}")
    """

# End ORA2 Tables ==================

class ForumsTask(CourseTask, MongoTask):
    NAME = ''
    QUERY = '{{"course_id": "{course}"}}'

    @classmethod
    def get_filename(cls, **kwargs):
        # The filename format of Forum exports is different for legacy reasons:
        # prod: "{course}.{extension}"
        # edge: "{course}-edge.{extension}"
        template = "{course}-{environment}.{extension}"

        filename = template.format(
            course=cls.get_course_name(kwargs['course']),
            environment=kwargs['environment'],
            extension=cls.EXT
        )
        return os.path.join(kwargs['work_dir'], filename)


class FindAllCoursesTask(DjangoAdminTask):
    NAME = 'courses'
    EXT = 'txt'
    COMMAND = 'dump_course_ids'
    ARGS = ''
    OUT = '{filename}'


class CourseStructureTask(CourseTask, DjangoAdminTask):
    NAME = 'course_structure'
    EXT = 'json'
    COMMAND = 'dump_course_structure'
    ARGS = '{course}'
    OUT = '{filename}'


class CourseContentTask(CourseTask, DjangoAdminTask):
    NAME = 'course'
    EXT = 'xml.tar.gz'
    COMMAND = 'export_olx'
    ARGS = '{course}'
    OUT = '{filename}'
    VARS = 'CONFIG_ROOT={django_config} SERVICE_VARIANT=cms'
    # Change CMD to use django_cms_settings.
    CMD = """
    sudo -E -u {django_user} {variables}
      {django_admin} {command}
      --settings={django_cms_settings}
      --pythonpath={django_pythonpath}
      {arguments}
    > {output}
    """


class OrgEmailOptInTask(OrgTask, DjangoAdminTask):
    NAME = 'email_opt_in'
    EXT = 'csv'
    COMMAND = 'email_opt_in_list'
    ARGS = '{organization} --courses={comma_sep_courses}'
    OUT = '{filename}'
    CMD = """
    sudo -E -u {django_user} {variables}
      {django_admin} {command}
      --settings={django_settings}
      --pythonpath={django_pythonpath}
      {output}
      {arguments}
    """

    @classmethod
    def run(cls, filename, dry_run, **kwargs):
        kwargs['comma_sep_courses'] = ','.join(kwargs['courses'])
        super(OrgEmailOptInTask, cls).run(filename, dry_run, **kwargs)


DEFAULT_TASKS = [
    UserIDMapTask,
    StudentModuleTask,
    TeamsTask,
    TeamsMembershipTask,
    CourseEnrollmentTask,
    GeneratedCertificateTask,
    InCourseReverificationTask,
    AuthUserTask,
    AuthUserProfileTask,
    StudentLanguageProficiencyTask,
    WikiArticleTask,
    WikiArticleRevisionTask,
    UserCourseTagTask,
    ForumsTask,
    CourseStructureTask,
    CourseContentTask,
    OrgEmailOptInTask,
    # To avoid confusing data czars while AI isn't usable, let's not export all the AI
    # tables. Leaving list here so we don't miss any when we're ready to export these.
    # AssessmentAIClassifierTask,
    # AssessmentAIClassifierSetTask,
    # AssessmentAIGradingWorkflowTask,
    # AssessmentAITrainingWorkflowTask,
    # AssessmentAITrainingWorkflowTrainingExamplesTask,
    AssessmentAssessmentTask,
    AssessmentAssessmentFeedbackTask,
    AssessmentAssessmentFeedbackAssessmentsTask,
    AssessmentAssessmentFeedbackOptionsTask,
    AssessmentAssessmentFeedbackOptionTask,
    AssessmentAssessmentPartTask,
    AssessmentCriterionTask,
    AssessmentCriterionOptionTask,
    AssessmentPeerWorkflowTask,
    AssessmentPeerWorkflowItemTask,
    AssessmentRubricTask,
    AssessmentStudentTrainingWorkflow,
    AssessmentStudentTrainingWorkflowItemTask,
    AssessmentTrainingExampleTask,
    AssessmentTrainingExampleOptionsSelectedTask,
    SubmissionsScoreTask,
    SubmissionsScoreSummaryTask,
    SubmissionsStudentItemTask,
    SubmissionsSubmissionTask,
    WorkflowAssessmentWorkflowTask,
    WorkflowAssessmentWorkflowStepTask,
    StudentAnonymousUserIDTask,
]
