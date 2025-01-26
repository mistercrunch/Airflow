# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import tempfile
from unittest import mock

import pytest

from airflow.configuration import conf
from airflow.operators.empty import EmptyOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.smtp.notifications.smtp import (
    SmtpNotifier,
    send_smtp_notification,
)
from airflow.utils import timezone

from tests_common.test_utils.version_compat import AIRFLOW_V_2_10_PLUS, AIRFLOW_V_3_0_PLUS

pytestmark = pytest.mark.db_test

SMTP_API_DEFAULT_CONN_ID = SmtpHook.default_conn_name


NUM_TRY = 0 if AIRFLOW_V_2_10_PLUS else 1


class TestSmtpNotifier:
    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier(self, mock_smtphook_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")
        notifier = send_smtp_notification(
            from_email="test_sender@test.com",
            to="test_reciver@test.com",
            subject="subject",
            html_content="body",
        )
        notifier({"dag": dag})
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email="test_sender@test.com",
            to="test_reciver@test.com",
            subject="subject",
            html_content="body",
            smtp_conn_id="smtp_default",
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            custom_headers=None,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_notifier_class(self, mock_smtphook_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")
        notifier = SmtpNotifier(
            from_email="test_sender@test.com",
            to="test_reciver@test.com",
            subject="subject",
            html_content="body",
        )
        notifier({"dag": dag})
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email="test_sender@test.com",
            to="test_reciver@test.com",
            subject="subject",
            html_content="body",
            smtp_conn_id="smtp_default",
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            custom_headers=None,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_templated(self, mock_smtphook_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")

        notifier = SmtpNotifier(
            from_email="test_sender@test.com {{dag.dag_id}}",
            to="test_reciver@test.com {{dag.dag_id}}",
            subject="subject {{dag.dag_id}}",
            html_content="body {{dag.dag_id}}",
        )
        context = {"dag": dag}
        notifier(context)
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email="test_sender@test.com test_notifier",
            to="test_reciver@test.com test_notifier",
            subject="subject test_notifier",
            html_content="body test_notifier",
            smtp_conn_id="smtp_default",
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            custom_headers=None,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_defaults(self, mock_smtphook_hook, create_task_instance):
        ti = create_task_instance(dag_id="dag", task_id="op", logical_date=timezone.datetime(2018, 1, 1))
        context = {"dag": ti.dag_run.dag, "ti": ti}
        with (
            tempfile.NamedTemporaryFile(mode="wt", suffix=".txt") as f_content,
        ):
            f_content.write("Mock content goes here")
            f_content.flush()
            mock_smtphook_hook.return_value.from_email = "default@email.com"
            mock_smtphook_hook.return_value.subject_template = None
            mock_smtphook_hook.return_value.html_content_template = f_content.name
            notifier = SmtpNotifier(
                to="test_reciver@test.com",
            )
            notifier(context)
            if AIRFLOW_V_3_0_PLUS:
                expected_from_email = "default@email.com"
            else:
                expected_from_email = conf.get("smtp", "smtp_mail_from")
            mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
                from_email=expected_from_email,
                to="test_reciver@test.com",
                subject="DAG dag - Task op - Run ID test in State None",
                html_content="Mock content goes here",
                smtp_conn_id="smtp_default",
                files=None,
                cc=None,
                bcc=None,
                mime_subtype="mixed",
                mime_charset="utf-8",
                custom_headers=None,
            )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_nondefault_conf_vars(self, mock_smtphook_hook, create_task_instance, caplog):
        ti = create_task_instance(dag_id="dag", task_id="op", logical_date=timezone.datetime(2018, 1, 1))
        context = {"dag": ti.dag_run.dag, "ti": ti}

        with (
            tempfile.NamedTemporaryFile(mode="wt", suffix=".txt") as f_subject,
            tempfile.NamedTemporaryFile(mode="wt", suffix=".txt") as f_content,
        ):
            f_subject.write("Task {{ ti.task_id }} failed")
            f_subject.flush()

            f_content.write("Mock content goes here")
            f_content.flush()

            mock_smtphook_hook.return_value.from_email = None
            mock_smtphook_hook.return_value.html_content_template = f_content.name
            mock_smtphook_hook.return_value.subject_template = f_subject.name

            notifier = SmtpNotifier(
                to="test_reciver@test.com",
            )

            notifier(context)
            if not AIRFLOW_V_3_0_PLUS:
                mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
                    from_email=conf.get("smtp", "smtp_mail_from"),
                    to="test_reciver@test.com",
                    subject="Task op failed",
                    html_content="Mock content goes here",
                    smtp_conn_id="smtp_default",
                    files=None,
                    cc=None,
                    bcc=None,
                    mime_subtype="mixed",
                    mime_charset="utf-8",
                    custom_headers=None,
                )
            else:
                assert (
                    "Failed to send notification: you must provide from_email argument,"
                    " or set a default one in the connection" in caplog.text
                )
                mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_not_called()
