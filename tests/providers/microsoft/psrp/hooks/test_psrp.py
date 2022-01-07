#
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
#

from logging import DEBUG, ERROR, INFO, WARNING
from unittest import TestCase
from unittest.mock import MagicMock, Mock, call, patch

from parameterized import parameterized
from pypsrp.messages import MessageType
from pypsrp.powershell import PSInvocationState
from pytest import raises

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.psrp.hooks.psrp import PsrpHook

CONNECTION_ID = "conn_id"
DUMMY_STACKTRACE = [
    r"at Invoke-Foo, C:\module.psm1: line 113",
    r"at Invoke-Bar, C:\module.psm1: line 125",
]


class MockPowerShell(MagicMock):
    had_errors = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = PSInvocationState.NOT_STARTED

    def poll_invoke(self, timeout=None):
        self.state = PSInvocationState.COMPLETED
        self.output.append("output")

        def informational(message_type, message, **kwargs):
            return Mock(MESSAGE_TYPE=message_type, command_name="command", message=message, **kwargs)

        self.streams.debug.append(informational(MessageType.DEBUG_RECORD, "debug"))
        self.streams.verbose.append(informational(MessageType.VERBOSE_RECORD, "verbose"))
        self.streams.warning.append(informational(MessageType.WARNING_RECORD, "warning"))
        self.streams.information.append(
            Mock(
                MESSAGE_TYPE=MessageType.INFORMATION_RECORD,
                computer="computer",
                user="user",
                message_data="information",
            )
        )
        self.streams.progress.append(
            Mock(MESSAGE_TYPE=MessageType.PROGRESS_RECORD, activity="activity", description="description")
        )

        if self.had_errors:
            # There seems to be cases where an error record does not have a
            # message type; in this case, we match on the class name as a
            # workaround.
            class ErrorRecord(Mock):
                pass

            self.streams.error.append(
                ErrorRecord(
                    MESSAGE_TYPE=None,
                    command_name="command",
                    message="error",
                    reason="reason",
                    script_stacktrace="\r\n".join(DUMMY_STACKTRACE),
                )
            )

    def begin_invoke(self):
        self.state = PSInvocationState.RUNNING
        self.output = []
        self.streams.debug = []
        self.streams.error = []
        self.streams.information = []
        self.streams.progress = []
        self.streams.verbose = []
        self.streams.warning = []

    def end_invoke(self):
        while self.state == PSInvocationState.RUNNING:
            self.poll_invoke()


def mock_powershell_factory():
    return MagicMock(return_value=MockPowerShell())


@patch(
    f"{PsrpHook.__module__}.{PsrpHook.__name__}.get_connection",
    new=lambda _, conn_id: Connection(
        conn_id=conn_id,
        login='username',
        password='password',
        host='remote_host',
    ),
)
@patch(f"{PsrpHook.__module__}.WSMan")
@patch(f"{PsrpHook.__module__}.PowerShell", new_callable=mock_powershell_factory)
@patch(f"{PsrpHook.__module__}.RunspacePool")
class TestPsrpHook(TestCase):
    def test_get_conn(self, runspace_pool, powershell, ws_man):
        hook = PsrpHook(CONNECTION_ID)
        assert hook.get_conn() is runspace_pool.return_value

    def test_get_conn_unexpected_extra(self, runspace_pool, powershell, ws_man):
        hook = PsrpHook(CONNECTION_ID)
        conn = hook.get_connection(CONNECTION_ID)

        def get_connection(*args):
            conn.extra = '{"foo": "bar"}'
            return conn

        hook.get_connection = get_connection
        with raises(AirflowException, match="Unexpected extra configuration keys: foo"):
            hook.get_conn()

    @parameterized.expand([(None,), (ERROR,)])
    def test_invoke(self, runspace_pool, powershell, ws_man, logging_level):
        runspace_options = {"connection_name": "foo"}
        wsman_options = {"encryption": "auto"}

        options = {}
        if logging_level is not None:
            options["logging_level"] = logging_level

        with PsrpHook(
            CONNECTION_ID, runspace_options=runspace_options, wsman_options=wsman_options, **options
        ) as hook, patch.object(type(hook), "log") as logger:
            try:
                with hook.invoke() as ps:
                    assert ps.state == PSInvocationState.NOT_STARTED

                    # We're simulating an error in order to test error
                    # handling as well as the logging of error exception
                    # details.
                    ps.had_errors = True
            except AirflowException as exc:
                assert str(exc) == 'Process had one or more errors'
            else:
                self.fail("Expected an error")
            assert ps.state == PSInvocationState.COMPLETED

        assert runspace_pool.return_value.__exit__.mock_calls == [call(None, None, None)]
        assert ws_man().__exit__.mock_calls == [call(None, None, None)]
        assert ws_man.call_args_list[0][1]["encryption"] == "auto"
        assert logger.method_calls[0] == call.setLevel(logging_level or DEBUG)

        def assert_log(level, *args):
            assert call.log(level, *args) in logger.method_calls

        assert_log(DEBUG, '%s: %s', 'command', 'debug')
        assert_log(ERROR, '%s: %s', 'command', 'error')
        assert_log(INFO, '%s: %s', 'command', 'verbose')
        assert_log(WARNING, '%s: %s', 'command', 'warning')
        assert_log(INFO, 'Progress: %s (%s)', 'activity', 'description')
        assert_log(INFO, '%s (%s): %s', 'computer', 'user', 'information')
        assert_log(INFO, '%s: %s', 'reason', ps.streams.error[0])
        assert_log(INFO, DUMMY_STACKTRACE[0])
        assert_log(INFO, DUMMY_STACKTRACE[1])

        assert call('Invocation state: %s', 'Completed') in logger.info.mock_calls

        assert runspace_pool.call_args == call(ws_man.return_value, connection_name='foo')

    def test_invoke_cmdlet(self, *mocks):
        with PsrpHook(CONNECTION_ID) as hook:
            ps = hook.invoke_cmdlet('foo', bar="1", baz="2")
            assert [call('foo', use_local_scope=None)] == ps.add_cmdlet.mock_calls
            assert [call({'bar': '1', 'baz': '2'})] == ps.add_parameters.mock_calls

    def test_invoke_powershell(self, *mocks):
        with PsrpHook(CONNECTION_ID) as hook:
            ps = hook.invoke_powershell('foo')
            assert call('foo') in ps.add_script.mock_calls

    def test_invoke_local_context(self, *mocks):
        hook = PsrpHook(CONNECTION_ID)
        ps = hook.invoke_powershell('foo')
        assert call('foo') in ps.add_script.mock_calls
