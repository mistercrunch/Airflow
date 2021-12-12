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

from contextlib import contextmanager
from typing import Any, Dict, Optional
from weakref import WeakKeyDictionary

from pypsrp.messages import (
    DebugRecord,
    ErrorRecord,
    InformationRecord,
    ProgressRecord,
    VerboseRecord,
    WarningRecord,
)
from pypsrp.powershell import PowerShell, PSInvocationState, RunspacePool
from pypsrp.wsman import WSMan

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class PSRPHook(BaseHook):
    """
    Hook for PowerShell Remoting Protocol execution.

    When used as a context manager, the runspace pool is reused between shell
    sessions.

    :param psrp_conn_id: Required. The name of the PSRP connection.
    :type psrp_conn_id: str
    :param logging: If true (default), log command output and streams during execution.
    :type logging: bool
    :param operation_timeout: Override the default WSMan timeout when polling the pipeline.
    :type operation_timeout: float
    :param runspace_options:
        Optional dictionary which is passed when creating the runspace pool. See
        :py:class:`~pypsrp.powershell.RunspacePool` for a description of the
        available options.
    :type runspace_options: dict
    :param wsman_options:
        Optional dictionary which is passed when creating the `WSMan` client. See
        :py:class:`~pypsrp.wsman.WSMan` for a description of the available options.
    :type wsman_options: dict
    :param exchange_keys:
        If true (default), automatically initiate a session key exchange when the
        hook is used as a context manager.
    :type exchange_keys: bool

    You can provide an alternative `configuration_name` using either `runspace_options`
    or by setting this key as the extra fields of your connection.
    """

    _conn = None
    _configuration_name = None
    _wsman_ref: "WeakKeyDictionary[RunspacePool, WSMan]" = WeakKeyDictionary()

    def __init__(
        self,
        psrp_conn_id: str,
        logging: bool = True,
        operation_timeout: Optional[float] = None,
        runspace_options: Optional[Dict[str, Any]] = None,
        wsman_options: Optional[Dict[str, Any]] = None,
        exchange_keys: bool = True,
    ):
        self.conn_id = psrp_conn_id
        self._logging = logging
        self._operation_timeout = operation_timeout
        self._runspace_options = runspace_options or {}
        self._wsman_options = wsman_options or {}
        self._exchange_keys = exchange_keys

    def __enter__(self):
        conn = self.get_conn()
        self._wsman_ref[conn].__enter__()
        conn.__enter__()
        if self._exchange_keys:
            conn.exchange_keys()
        self._conn = conn
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self._conn.__exit__(exc_type, exc_value, traceback)
            self._wsman_ref[self._conn].__exit__(exc_type, exc_value, traceback)
        finally:
            del self._conn

    def get_conn(self) -> RunspacePool:
        """
        Returns a runspace pool.

        The returned object must be used as a context manager.
        """
        conn = self.get_connection(self.conn_id)
        self.log.info("Establishing WinRM connection %s to host: %s", self.conn_id, conn.host)

        extra = conn.extra_dejson.copy()

        def apply_extra(d, keys):
            d = d.copy()
            for key in keys:
                value = extra.pop(key, None)
                if value is not None:
                    d[key] = value
            return d

        wsman_options = apply_extra(
            self._wsman_options,
            (
                "auth",
                "cert_validation",
                "connection_timeout",
                "locale",
                "read_timeout",
                "reconnection_retries",
                "reconnection_backoff",
                "ssl",
            ),
        )
        wsman = WSMan(conn.host, username=conn.login, password=conn.password, **wsman_options)
        runspace_options = apply_extra(self._runspace_options, ("configuration_name",))

        if extra:
            raise AirflowException(f"Unexpected extra configuration keys: {', '.join(sorted(extra))}")
        pool = RunspacePool(wsman, **runspace_options)
        self._wsman_ref[pool] = wsman
        return pool

    @contextmanager
    def invoke(self) -> PowerShell:
        """
        Context manager that yields a PowerShell object to which commands can be
        added. Upon exit, the commands will be invoked.
        """
        local_context = self._conn is None
        if local_context:
            self.__enter__()
        try:
            ps = PowerShell(self._conn)
            yield ps
            ps.begin_invoke()
            if self._logging:
                streams = [
                    (ps.streams.debug, self._log_record),
                    (ps.streams.error, self._log_record),
                    (ps.streams.information, self._log_record),
                    (ps.streams.progress, self._log_record),
                    (ps.streams.verbose, self._log_record),
                    (ps.streams.warning, self._log_record),
                ]
                offsets = [0 for _ in streams]

                # We're using polling to make sure output and streams are
                # handled while the process is running.
                while ps.state == PSInvocationState.RUNNING:
                    ps.poll_invoke(timeout=self._operation_timeout)

                    for (i, (stream, handler)) in enumerate(streams):
                        offset = offsets[i]
                        while len(stream) > offset:
                            handler(stream[offset])
                            offset += 1
                        offsets[i] = offset

            # For good measure, we'll make sure the process has
            # stopped running in any case.
            ps.end_invoke()

            if ps.streams.error:
                raise AirflowException("Process had one or more errors")

            self.log.info("Invocation state: %s", str(PSInvocationState(ps.state)))
        finally:
            if local_context:
                self.__exit__(None, None, None)

    def invoke_cmdlet(self, name: str, use_local_scope=None, **parameters: Dict[str, str]) -> PowerShell:
        """Invoke a PowerShell cmdlet and return session."""
        with self.invoke() as ps:
            ps.add_cmdlet(name, use_local_scope=use_local_scope)
            ps.add_parameters(parameters)
        return ps

    def invoke_powershell(self, script: str) -> PowerShell:
        """Invoke a PowerShell script and return session."""
        with self.invoke() as ps:
            ps.add_script(script)
        return ps

    def _log_record(self, record):
        if isinstance(record, DebugRecord):
            self.log.debug("%s: %s", record.command_name, record.message)
            return

        if isinstance(record, ErrorRecord):
            self.log.error("%s: %s", record.command_name, record.message)
            return

        if isinstance(record, VerboseRecord):
            self.log.info("%s: %s", record.command_name, record.message)
            return

        if isinstance(record, WarningRecord):
            self.log.warning("%s: %s", record.command_name, record.message)
            return

        if isinstance(record, InformationRecord):
            self.log.info("%s (%s): %s", record.computer, record.user, record.message_data)
            return

        if isinstance(record, ProgressRecord):
            self.log.info("Progress: %s (%s)", record.activity, record.description)
            return

        self.log.warning("Unsupported record type: %s", type(record).__name__)
