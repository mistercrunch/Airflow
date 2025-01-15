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

import importlib
import logging
import os
import signal
import threading
import time
import zipfile
from contextlib import contextmanager, redirect_stderr, redirect_stdout, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator, Iterable

from setproctitle import setproctitle
from sqlalchemy import delete, event

from airflow import settings
from airflow.api_internal.internal_api_call import internal_api_call
from airflow.callbacks.callback_requests import (
    DagCallbackRequest,
    TaskCallbackRequest,
)
from airflow.configuration import conf
from airflow.dag_processing.dag_store import DagStore
from airflow.dag_processing.dag_importer import DagImporter
from airflow.exceptions import AirflowException
from airflow.listeners.listener import get_listener_manager
from airflow.models.dag import DAG, DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagwarning import DagWarning, DagWarningType
from airflow.models.errors import ParseImportError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance, _run_finished_callback
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.file import iter_airflow_imports, might_contain_dag
from airflow.utils.log.logging_mixin import LoggingMixin, StreamLogWriter, set_context
from airflow.utils.mixins import MultiprocessingStartMethodMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    import multiprocessing
    from datetime import datetime
    from multiprocessing.connection import Connection as MultiprocessingConnection

    from sqlalchemy.orm.session import Session

    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.models.operator import Operator


@dataclass
class _QueryCounter:
    queries_number: int = 0

    def inc(self):
        self.queries_number += 1


@contextmanager
def count_queries(session: Session) -> Generator[_QueryCounter, None, None]:
    # using list allows to read the updated counter from what context manager returns
    counter: _QueryCounter = _QueryCounter()

    @event.listens_for(session, "do_orm_execute")
    def _count_db_queries(orm_execute_state):
        nonlocal counter
        counter.inc()

    yield counter
    event.remove(session, "do_orm_execute", _count_db_queries)


class DagFileProcessorProcess(LoggingMixin, MultiprocessingStartMethodMixin):
    """
    Runs DAG processing in a separate process using DagFileProcessor.

    :param file_path: a Python file containing Airflow DAG definitions
    :param pickle_dags: whether to serialize the DAG objects to the DB
    :param dag_ids: If specified, only look at these DAG ID's
    :param callback_requests: failure callback to execute
    """

    # Counter that increments every time an instance of this class is created
    class_creation_counter = 0

    def __init__(
        self,
        file_path: str,
        pickle_dags: bool,
        dag_ids: list[str] | None,
        importer_path: str,
        importer: DagImporter,
        dag_store: DagStore,
        callback_requests: list[CallbackRequest],
    ):
        super().__init__()
        self._file_path = file_path
        self._pickle_dags = pickle_dags
        self._dag_ids = dag_ids
        self._importer_path = importer_path
        self._importer = importer
        self._dag_store = dag_store
        self._callback_requests = callback_requests

        # The process that was launched to process the given .
        self._process: multiprocessing.process.BaseProcess | None = None
        # The result of DagFileProcessor.process_file(file_path).
        self._result: tuple[int, int, int] | None = None
        # Whether the process is done running.
        self._done = False
        # When the process started.
        self._start_time: datetime | None = None
        # This ID is use to uniquely name the process / thread that's launched
        # by this processor instance
        self._instance_id = DagFileProcessorProcess.class_creation_counter

        self._parent_channel: MultiprocessingConnection | None = None
        DagFileProcessorProcess.class_creation_counter += 1

    @property
    def file_path(self) -> str:
        return self._file_path

    @staticmethod
    def _run_file_processor(
        result_channel: MultiprocessingConnection,
        parent_channel: MultiprocessingConnection,
        file_path: str,
        pickle_dags: bool,
        dag_ids: list[str] | None,
        thread_name: str,
        importer_path: str,
        importer: DagImporter,
        dag_store: DagStore,
        callback_requests: list[CallbackRequest],
    ) -> None:
        """
        Process the given file.

        :param result_channel: the connection to use for passing back the result
        :param parent_channel: the parent end of the channel to close in the child
        :param file_path: the file to process
        :param pickle_dags: whether to pickle the DAGs found in the file and
            save them to the DB
        :param dag_ids: if specified, only examine DAG ID's that are
            in this list
        :param thread_name: the name to use for the process that is launched
        :param callback_requests: failure callback to execute
        :return: the process that was launched
        """
        # This helper runs in the newly created process
        log: logging.Logger = logging.getLogger("airflow.processor")

        # Since we share all open FDs from the parent, we need to close the parent side of the pipe here in
        # the child, else it won't get closed properly until we exit.
        parent_channel.close()
        del parent_channel

        set_context(log, file_path)
        setproctitle(f"airflow scheduler - DagFileProcessor {file_path}")

        def _handle_dag_file_processing():
            # Re-configure the ORM engine as there are issues with multiple processes
            settings.configure_orm()

            # Change the thread name to differentiate log lines. This is
            # really a separate process, but changing the name of the
            # process doesn't work, so changing the thread name instead.
            threading.current_thread().name = thread_name

            log.info("Started process (PID=%s) to work on %s", os.getpid(), file_path)
            dag_file_processor = DagFileProcessor(dag_ids=dag_ids, importer_path=importer_path, importer=importer, dag_store=dag_store, log=log)
            result: tuple[int, int, int] = dag_file_processor.process_file(
                file_path=file_path,
                pickle_dags=pickle_dags,
                callback_requests=callback_requests,
            )
            result_channel.send(result)

        try:
            DAG_PROCESSOR_LOG_TARGET = conf.get_mandatory_value("logging", "DAG_PROCESSOR_LOG_TARGET")
            if DAG_PROCESSOR_LOG_TARGET == "stdout":
                with Stats.timer() as timer:
                    _handle_dag_file_processing()
            else:
                # The following line ensures that stdout goes to the same destination as the logs. If stdout
                # gets sent to logs and logs are sent to stdout, this leads to an infinite loop. This
                # necessitates this conditional based on the value of DAG_PROCESSOR_LOG_TARGET.
                with redirect_stdout(StreamLogWriter(log, logging.INFO)), redirect_stderr(
                    StreamLogWriter(log, logging.WARNING)
                ), Stats.timer() as timer:
                    _handle_dag_file_processing()
            log.info("Processing %s took %.3f seconds", file_path, timer.duration)
        except Exception:
            # Log exceptions through the logging framework.
            log.exception("Got an exception! Propagating...")
            raise
        finally:
            # We re-initialized the ORM within this Process above so we need to
            # tear it down manually here
            settings.dispose_orm()

            result_channel.close()

    def start(self) -> None:
        """Launch the process and start processing the DAG."""
        # TODO: Introduce pre-import optimization at the level of the importer.
        # if conf.getboolean("scheduler", "parsing_pre_import_modules", fallback=True):

            # Read the file to pre-import airflow modules used.
            # This prevents them from being re-imported from zero in each "processing" process
            # and saves CPU time and memory.
        #     zip_file_paths = []
        #     if zipfile.is_zipfile(self.file_path):
        #         try:
        #             with zipfile.ZipFile(self.file_path) as z:
        #                 zip_file_paths.extend(
        #                     [
        #                         os.path.join(self.file_path, info.filename)
        #                         for info in z.infolist()
        #                         if might_contain_dag(info.filename, True, z)
        #                     ]
        #                 )
        #         except zipfile.BadZipFile as err:
        #             self.log.error("There was an err accessing %s, %s", self.file_path, err)
        #     if zip_file_paths:
        #         self.import_modules(zip_file_paths)
        #     else:
        #         self.import_modules(self.file_path)

        context = self._get_multiprocessing_context()

        _parent_channel, _child_channel = context.Pipe(duplex=False)
        process = context.Process(
            target=type(self)._run_file_processor,
            args=(
                _child_channel,
                _parent_channel,
                self.file_path,
                self._pickle_dags,
                self._dag_ids,
                f"DagFileProcessor{self._instance_id}",
                self._importer_path,
                self._importer,
                self._dag_store,
                self._callback_requests,
            ),
            name=f"DagFileProcessor{self._instance_id}-Process",
        )
        self._process = process
        self._start_time = timezone.utcnow()
        process.start()

        # Close the child side of the pipe now the subprocess has started -- otherwise this would prevent it
        # from closing in some cases
        _child_channel.close()
        del _child_channel

        # Don't store it on self until after we've started the child process - we don't want to keep it from
        # getting GCd/closed
        self._parent_channel = _parent_channel

    def kill(self) -> None:
        """Kill the process launched to process the file, and ensure consistent state."""
        if self._process is None:
            raise AirflowException("Tried to kill before starting!")
        self._kill_process()

    def terminate(self, sigkill: bool = False) -> None:
        """
        Terminate (and then kill) the process launched to process the file.

        :param sigkill: whether to issue a SIGKILL if SIGTERM doesn't work.
        """
        if self._process is None or self._parent_channel is None:
            raise AirflowException("Tried to call terminate before starting!")

        self._process.terminate()
        # Arbitrarily wait 5s for the process to die
        with suppress(TimeoutError):
            self._process._popen.wait(5)  # type: ignore
        if sigkill:
            self._kill_process()
        self._parent_channel.close()

    def _kill_process(self) -> None:
        if self._process is None:
            raise AirflowException("Tried to kill process before starting!")

        if self._process.is_alive() and self._process.pid:
            self.log.warning("Killing DAGFileProcessorProcess (PID=%d)", self._process.pid)
            os.kill(self._process.pid, signal.SIGKILL)

            # Reap the spawned zombie. We active wait, because in Python 3.9 `waitpid` might lead to an
            # exception, due to change in Python standard library and possibility of race condition
            # see https://bugs.python.org/issue42558
            while self._process._popen.poll() is None:  # type: ignore
                time.sleep(0.001)
        if self._parent_channel:
            self._parent_channel.close()

    @property
    def pid(self) -> int:
        """PID of the process launched to process the given file."""
        if self._process is None or self._process.pid is None:
            raise AirflowException("Tried to get PID before starting!")
        return self._process.pid

    @property
    def exit_code(self) -> int | None:
        """
        After the process is finished, this can be called to get the return code.

        :return: the exit code of the process
        """
        if self._process is None:
            raise AirflowException("Tried to get exit code before starting!")
        if not self._done:
            raise AirflowException("Tried to call retcode before process was finished!")
        return self._process.exitcode

    @property
    def done(self) -> bool:
        """
        Check if the process launched to process this file is done.

        :return: whether the process is finished running
        """
        if self._process is None or self._parent_channel is None:
            raise AirflowException("Tried to see if it's done before starting!")

        if self._done:
            return True

        if self._parent_channel.poll():
            try:
                self._result = self._parent_channel.recv()
                self._done = True
                self.log.debug("Waiting for %s", self._process)
                self._process.join()
                self._parent_channel.close()
                return True
            except EOFError:
                # If we get an EOFError, it means the child end of the pipe has been closed. This only happens
                # in the finally block. But due to a possible race condition, the process may have not yet
                # terminated (it could be doing cleanup/python shutdown still). So we kill it here after a
                # "suitable" timeout.
                self._done = True
                # Arbitrary timeout -- error/race condition only, so this doesn't need to be tunable.
                self._process.join(timeout=5)
                if self._process.is_alive():
                    # Didn't shut down cleanly - kill it
                    self._kill_process()

        if not self._process.is_alive():
            self._done = True
            self.log.debug("Waiting for %s", self._process)
            self._process.join()
            self._parent_channel.close()
            return True

        return False

    @property
    def result(self) -> tuple[int, int, int] | None:
        """Result of running ``DagFileProcessor.process_file()``."""
        if not self.done:
            raise AirflowException("Tried to get the result before it's done!")
        return self._result

    @property
    def start_time(self) -> datetime:
        """Time when this started to process the file."""
        if self._start_time is None:
            raise AirflowException("Tried to get start time before it started!")
        return self._start_time

    @property
    def waitable_handle(self):
        return self._process.sentinel

    def import_modules(self, file_path: str | Iterable[str]):
        def _import_modules(filepath):
            for module in iter_airflow_imports(filepath):
                try:
                    importlib.import_module(module)
                except Exception as e:
                    # only log as warning because an error here is not preventing anything from working, and
                    # if it's serious, it's going to be surfaced to the user when the dag is actually parsed.
                    self.log.warning(
                        "Error when trying to pre-import module '%s' found in %s: %s",
                        module,
                        file_path,
                        e,
                    )

        if isinstance(file_path, str):
            _import_modules(file_path)
        elif isinstance(file_path, Iterable):
            for path in file_path:
                _import_modules(path)


class DagFileProcessor(LoggingMixin):
    """
    Process a Python file containing Airflow DAGs.

    This includes:

    1. Execute the file and look for DAG objects in the namespace.
    2. Execute any Callbacks if passed to DagFileProcessor.process_file
    3. Serialize the DAGs and save it to DB (or update existing record in the DB).
    4. Pickle the DAG and save it to the DB (if necessary).
    5. Record any errors importing the file into ORM

    Returns a tuple of 'number of dags found' and 'the count of import errors'

    :param dag_ids: If specified, only look at these DAG ID's
    :param log: Logger to save the processing process
    """

    UNIT_TEST_MODE: bool = conf.getboolean("core", "UNIT_TEST_MODE")

    def __init__(self, dag_ids: list[str] | None, importer_path: str, importer: DagImporter, dag_store: DagStore, log: logging.Logger):
        super().__init__()
        self.dag_ids = dag_ids
        self._log = log
        self._dag_store = dag_store
        self._importer_path = importer_path
        self._importer = importer
        self.dag_warnings: set[tuple[str, str]] = set()
        self._last_num_of_db_queries = 0

    @staticmethod
    @internal_api_call
    @provide_session
    def update_import_errors(
        file_last_changed: dict[str, datetime],
        import_errors: dict[str, str],
        processor_subdir: str | None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Update any import errors to be displayed in the UI.

        For the DAGs in the given DagBag, record any associated import errors and clears
        errors for files that no longer have them. These are usually displayed through the
        Airflow UI so that users know that there are issues parsing DAGs.
        :param file_last_changed: Dictionary containing the last changed time of the files
        :param import_errors: Dictionary containing the import errors
        :param session: session for ORM operations
        """
        files_without_error = file_last_changed - import_errors.keys()

        # Clear the errors of the processed files
        # that no longer have errors
        for dagbag_file in files_without_error:
            session.execute(
                delete(ParseImportError)
                .where(ParseImportError.filename.startswith(dagbag_file))
                .execution_options(synchronize_session="fetch")
            )

        # files that still have errors
        existing_import_error_files = [x.filename for x in session.query(ParseImportError.filename).all()]

        # Add the errors of the processed files
        for filename, stacktrace in import_errors.items():
            if filename in existing_import_error_files:
                session.query(ParseImportError).filter(ParseImportError.filename == filename).update(
                    {"filename": filename, "timestamp": timezone.utcnow(), "stacktrace": stacktrace},
                    synchronize_session="fetch",
                )
                # sending notification when an existing dag import error occurs
                get_listener_manager().hook.on_existing_dag_import_error(
                    filename=filename, stacktrace=stacktrace
                )
            else:
                session.add(
                    ParseImportError(
                        filename=filename,
                        timestamp=timezone.utcnow(),
                        stacktrace=stacktrace,
                        processor_subdir=processor_subdir,
                    )
                )
                # sending notification when a new dag import error occurs
                get_listener_manager().hook.on_new_dag_import_error(filename=filename, stacktrace=stacktrace)
            (
                session.query(DagModel)
                .filter(DagModel.fileloc == filename)
                .update({"has_import_errors": True}, synchronize_session="fetch")
            )

        session.commit()
        session.flush()

    @classmethod
    def update_dag_warnings(cla, *, dagbag: DagBag) -> None:
        """Validate and raise exception if any task in a dag is using a non-existent pool."""

        def get_pools(dag) -> dict[str, set[str]]:
            return {dag.dag_id: {task.pool for task in dag.tasks}}

        pool_dict: dict[str, set[str]] = {}
        for dag in dagbag.dags.values():
            pool_dict.update(get_pools(dag))
        dag_ids = {dag.dag_id for dag in dagbag.dags.values()}
        return DagFileProcessor._validate_task_pools_and_update_dag_warnings(pool_dict, dag_ids)

    @classmethod
    @internal_api_call
    @provide_session
    def _validate_task_pools_and_update_dag_warnings(
        cls, pool_dict: dict[str, set[str]], dag_ids: set[str], session: Session = NEW_SESSION
    ) -> None:
        from airflow.models.pool import Pool

        all_pools = {p.pool for p in Pool.get_pools(session)}
        warnings: set[DagWarning] = set()
        for dag_id, dag_pools in pool_dict.items():
            nonexistent_pools = dag_pools - all_pools
            if nonexistent_pools:
                warnings.add(
                    DagWarning(
                        dag_id,
                        DagWarningType.NONEXISTENT_POOL,
                        f"Dag '{dag_id}' references non-existent pools: {sorted(nonexistent_pools)!r}",
                    )
                )

        stored_warnings = set(session.query(DagWarning).filter(DagWarning.dag_id.in_(dag_ids)).all())

        for warning_to_delete in stored_warnings - warnings:
            session.delete(warning_to_delete)

        for warning_to_add in warnings:
            session.merge(warning_to_add)
        session.flush()
        session.commit()

    @classmethod
    @internal_api_call
    @provide_session
    def execute_callbacks(
        cls,
        dagbag: DagBag,
        callback_requests: list[CallbackRequest],
        unit_test_mode: bool,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Execute on failure callbacks.

        These objects can come from SchedulerJobRunner or from DagProcessorJobRunner.

        :param dagbag: Dag Bag of dags
        :param callback_requests: failure callbacks to execute
        :param session: DB session.

        :return: number of queries executed
        """
        for request in callback_requests:
            cls.logger().debug("Processing Callback Request: %s", request)
            try:
                if isinstance(request, TaskCallbackRequest):
                    cls._execute_task_callbacks(dagbag, request, unit_test_mode, session=session)
                elif isinstance(request, DagCallbackRequest):
                    cls._execute_dag_callbacks(dagbag, request, session=session)
            except Exception:
                cls.logger().exception(
                    "Error executing %s callback for file: %s",
                    request.__class__.__name__,
                    request.full_filepath,
                )
        session.flush()
        session.commit()

    @classmethod
    @internal_api_call
    @provide_session
    def execute_callbacks_without_dag(
        cls, callback_requests: list[CallbackRequest], unit_test_mode: bool, session: Session = NEW_SESSION
    ) -> None:
        """
        Execute what callbacks we can as "best effort" when the dag cannot be found/had parse errors.

        This is so important so that tasks that failed when there is a parse
        error don't get stuck in queued state.
        """
        for request in callback_requests:
            cls.logger().debug("Processing Callback Request: %s", request)
            if isinstance(request, TaskCallbackRequest):
                cls._execute_task_callbacks(None, request, unit_test_mode, session)
            else:
                cls.logger().info(
                    "Not executing %s callback for file %s as there was a dag parse error",
                    request.__class__.__name__,
                    request.full_filepath,
                )
        session.flush()
        session.commit()

    @classmethod
    def _execute_dag_callbacks(cls, dagbag: DagBag, request: DagCallbackRequest, session: Session):
        dag = dagbag.dags[request.dag_id]
        callbacks, context = DAG.fetch_callback(
            dag=dag,
            dag_run_id=request.run_id,
            success=not request.is_failure_callback,
            reason=request.msg,
            session=session,
        ) or (None, None)

        if callbacks and context:
            DAG.execute_callback(callbacks, context, dag.dag_id)

    @classmethod
    @internal_api_call
    @provide_session
    def _execute_task_callbacks(
        cls, dagbag: DagBag | None, request: TaskCallbackRequest, unit_test_mode: bool, session: Session
    ) -> None:
        """
        Execute the task callbacks.

        :param dagbag: the DagBag to use to get the task instance
        :param request: the task callback request
        :param session: the session to use
        """
        try:
            callback_type = TaskInstanceState(request.task_callback_type)
        except ValueError:
            callback_type = None
        is_remote = callback_type in (TaskInstanceState.SUCCESS, TaskInstanceState.FAILED)

        # previously we ignored any request besides failures. now if given callback type directly,
        # then we respect it and execute it. additionally because in this scenario the callback
        # is submitted remotely, we assume there is no need to mess with state; we simply run
        # the callback

        if not is_remote and not request.is_failure_callback:
            return

        simple_ti = request.simple_task_instance
        ti = TaskInstance.get_task_instance(
            dag_id=simple_ti.dag_id,
            run_id=simple_ti.run_id,
            task_id=simple_ti.task_id,
            map_index=simple_ti.map_index,
            session=session,
        )

        if not ti:
            return

        task: Operator | None = None

        if dagbag and simple_ti.dag_id in dagbag.dags:
            dag = dagbag.dags[simple_ti.dag_id]
            if simple_ti.task_id in dag.task_ids:
                task = dag.get_task(simple_ti.task_id)
        else:
            # We don't have the _real_ dag here (perhaps it had a parse error?) but we still want to run
            # `handle_failure` so that the state of the TI gets progressed.
            #
            # Since handle_failure _really_ wants a task, we do our best effort to give it one
            task = SerializedDagModel.get_serialized_dag(
                dag_id=simple_ti.dag_id, task_id=simple_ti.task_id, session=session
            )

        if task:
            ti.refresh_from_task(task)

        if callback_type is TaskInstanceState.SUCCESS:
            context = ti.get_template_context(session=session)
            if TYPE_CHECKING:
                assert ti.task
            callbacks = ti.task.on_success_callback
            _run_finished_callback(callbacks=callbacks, context=context)
            cls.logger().info("Executed callback for %s in state %s", ti, ti.state)
        elif not is_remote or callback_type is TaskInstanceState.FAILED:
            ti.handle_failure(error=request.msg, test_mode=unit_test_mode, session=session)
            cls.logger().info("Executed callback for %s in state %s", ti, ti.state)
        session.flush()

    @provide_session
    def process_file(
        self,
        file_path: str,
        callback_requests: list[CallbackRequest],
        pickle_dags: bool = False,
        session: Session = NEW_SESSION,
    ) -> tuple[int, int, int]:
        """
        Process a Python file containing Airflow DAGs.

        This includes:

        1. Execute the file and look for DAG objects in the namespace.
        2. Execute any Callbacks if passed to this method.
        3. Serialize the DAGs and save it to DB (or update existing record in the DB).
        4. Pickle the DAG and save it to the DB (if necessary).
        5. Mark any DAGs which are no longer present as inactive
        6. Record any errors importing the file into ORM

        :param file_path: the path to the Python file that should be executed
        :param callback_requests: failure callback to execute
        :param pickle_dags: whether serialize the DAGs found in the file and
            save them to the db
        :return: number of dags found, count of import errors, last number of db queries
        """
        self.log.info("Processing file %s for tasks to queue", file_path)

        with count_queries(session) as query_counter:
            path_import_errors = {}
            imported_dags = {}
            try:
                for results in self._importer.import_path(file_path):
                    if results.dags:
                        imported_dags.update(**results.dags)

                    path_import_errors.update(**results.import_errors)
            except Exception:
                self.log.exception("Failed at reloading the DAG file %s", file_path)
                Stats.incr("dag_file_refresh_error", 1, 1, tags={"file_path": file_path})
                return 0, 0, self._cache_last_num_of_db_queries(query_counter)

            self._dag_store.delete_excluded_import_errors(path_import_errors, processor_subdir=self._importer_path,
                filepath_prefix=file_path)
            self._dag_store.delete_removed_entries_dags(set(dag.fileloc for dag in imported_dags.values()), processor_subdir=self._importer_path,
                filepath_prefix=file_path)

            if imported_dags:
                self.log.info("DAG(s) %s retrieved from %s", ", ".join(map(repr, imported_dags.values())), file_path)
            else:
                self.log.warning("No viable dags retrieved from %s", file_path)
                self._dag_store.update_import_errors(path_import_errors, self._importer_path)
                if callback_requests:
                    # If there were callback requests for this file but there was a
                    # parse error we still need to progress the state of TIs,
                    # otherwise they might be stuck in queued/running for ever!
                    DagFileProcessor.execute_callbacks_without_dag(callback_requests, self.UNIT_TEST_MODE)
                return 0, len(path_import_errors), self._cache_last_num_of_db_queries(query_counter)

            # TODO: Restore callbacks support
            # self.execute_callbacks(dagbag, callback_requests, self.UNIT_TEST_MODE)

            store_errors = self._dag_store.store_dags(imported_dags.values(), processor_subdir=self._importer_path)
            
            path_import_errors.update({imported_dags[dag_id].fileloc: store_errors[dag_id] for dag_id in store_errors})

            # Record import errors into the ORM
            try:
                self._dag_store.update_import_errors(path_import_errors, self._importer_path)
            except Exception:
                self.log.exception("Error logging import errors!")

            # Record DAG warnings in the metadatabase.
            try:
                # TODO: Restore warnings support in dag store
                # self.update_dag_warnings(dagbag=dagbag)
                pass
            except Exception:
                self.log.exception("Error logging DAG warnings.")

        return len(imported_dags), len(path_import_errors), self._cache_last_num_of_db_queries(query_counter)

    def _cache_last_num_of_db_queries(self, query_counter: _QueryCounter | None = None):
        if query_counter:
            self._last_num_of_db_queries = query_counter.queries_number
        return self._last_num_of_db_queries
