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
"""
Base operator for all operators.

:sphinx-autoapi-skip:
"""

from __future__ import annotations

import functools
import logging
import operator
from collections.abc import Collection, Iterable, Iterator, Sequence
from datetime import datetime, timedelta
from functools import singledispatchmethod, wraps
from threading import local
from types import FunctionType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    NoReturn,
    TypeVar,
)

import methodtools
import pendulum
from sqlalchemy import select
from sqlalchemy.orm.exc import NoResultFound

from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    TaskDeferralError,
    TaskDeferralTimeout,
    TaskDeferred,
)
from airflow.lineage import apply_lineage, prepare_lineage

# Keeping this file at all is a temp thing as we migrate the repo to the task sdk as the base, but to keep
# main working and useful for others to develop against we use the TaskSDK here but keep this file around
from airflow.models.abstractoperator import (
    AbstractOperator,
    NotMapped,
)
from airflow.models.base import _sentinel
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.models.taskmixin import DependencyMixin
from airflow.models.trigger import TRIGGER_FAIL_REPR, TriggerFailureReason
from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator as TaskSDKAbstractOperator
from airflow.sdk.definitions.baseoperator import (
    BaseOperatorMeta as TaskSDKBaseOperatorMeta,
    get_merged_defaults as get_merged_defaults,  # Re-export for compat
)
from airflow.sdk.definitions.context import Context
from airflow.sdk.definitions.dag import BaseOperator as TaskSDKBaseOperator
from airflow.sdk.definitions.edges import EdgeModifier as TaskSDKEdgeModifier
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.serialization.enums import DagAttributeTypes
from airflow.ti_deps.deps.mapped_task_upstream_dep import MappedTaskUpstreamDep
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils import timezone
from airflow.utils.context import context_get_outlet_events
from airflow.utils.edgemodifier import EdgeModifier
from airflow.utils.operator_helpers import ExecutionCallableRunner
from airflow.utils.operator_resources import Resources
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.abstractoperator import TaskStateChangeCallback
    from airflow.models.baseoperatorlink import BaseOperatorLink
    from airflow.models.dag import DAG as SchedulerDAG
    from airflow.models.operator import Operator
    from airflow.sdk.definitions.node import DAGNode
    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
    from airflow.triggers.base import BaseTrigger, StartTriggerArgs

TaskPreExecuteHook = Callable[[Context], None]
TaskPostExecuteHook = Callable[[Context, Any], None]

T = TypeVar("T", bound=FunctionType)

logger = logging.getLogger("airflow.models.baseoperator.BaseOperator")


def parse_retries(retries: Any) -> int | None:
    if retries is None:
        return 0
    elif type(retries) == int:  # noqa: E721
        return retries
    try:
        parsed_retries = int(retries)
    except (TypeError, ValueError):
        raise AirflowException(f"'retries' type must be int, not {type(retries).__name__}")
    logger.warning("Implicitly converting 'retries' from %r to int", retries)
    return parsed_retries


def coerce_timedelta(value: float | timedelta, *, key: str | None = None) -> timedelta:
    if isinstance(value, timedelta):
        return value
    # TODO: remove this log here
    if key:
        logger.debug("%s isn't a timedelta object, assuming secs", key)
    return timedelta(seconds=value)


def coerce_resources(resources: dict[str, Any] | None) -> Resources | None:
    if resources is None:
        return None
    return Resources(**resources)


class ExecutorSafeguard:
    """
    The ExecutorSafeguard decorator.

    Checks if the execute method of an operator isn't manually called outside
    the TaskInstance as we want to avoid bad mixing between decorated and
    classic operators.
    """

    test_mode = conf.getboolean("core", "unit_test_mode")
    _sentinel = local()
    _sentinel.callers = {}

    @classmethod
    def decorator(cls, func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            from airflow.decorators.base import DecoratedOperator
            from airflow.models.iterableoperator import IterableOperator

            sentinel_key = f"{self.__class__.__name__}__sentinel"
            sentinel = kwargs.pop(sentinel_key, None)

            if sentinel:
                if not getattr(cls._sentinel, "callers", None):
                    cls._sentinel.callers = {}
                cls._sentinel.callers[sentinel_key] = sentinel
            else:
                sentinel = cls._sentinel.callers.pop(f"{func.__qualname__.split('.')[0]}__sentinel", None)

            if (
                not cls.test_mode
                and not sentinel == _sentinel
                and not isinstance(self, DecoratedOperator)
                and not isinstance(self, IterableOperator)
            ):
                message = f"{self.__class__.__name__}.{func.__name__} cannot be called outside TaskInstance!"
                if not self.allow_nested_operators:
                    raise AirflowException(message)
                self.log.warning(message)
            return func(self, *args, **kwargs)

        return wrapper


# TODO: Task-SDK - temporarily extend the metaclass to add in the ExecutorSafeguard.
class BaseOperatorMeta(TaskSDKBaseOperatorMeta):
    """:meta private:"""  # noqa: D400

    def __new__(cls, name, bases, namespace, **kwargs):
        execute_method = namespace.get("execute")
        if callable(execute_method) and not getattr(execute_method, "__isabstractmethod__", False):
            namespace["execute"] = ExecutorSafeguard().decorator(execute_method)
        new_cls = super().__new__(cls, name, bases, namespace, **kwargs)
        return new_cls


class BaseOperator(TaskSDKBaseOperator, AbstractOperator, metaclass=BaseOperatorMeta):
    r"""
    Abstract base class for all operators.

    Since operators create objects that become nodes in the DAG, BaseOperator
    contains many recursive methods for DAG crawling behavior. To derive from
    this class, you are expected to override the constructor and the 'execute'
    method.

    Operators derived from this class should perform or trigger certain tasks
    synchronously (wait for completion). Example of operators could be an
    operator that runs a Pig job (PigOperator), a sensor operator that
    waits for a partition to land in Hive (HiveSensorOperator), or one that
    moves data from Hive to MySQL (Hive2MySqlOperator). Instances of these
    operators (tasks) target specific operations, running specific scripts,
    functions or data transfers.

    This class is abstract and shouldn't be instantiated. Instantiating a
    class derived from this one results in the creation of a task object,
    which ultimately becomes a node in DAG objects. Task dependencies should
    be set by using the set_upstream and/or set_downstream methods.

    :param task_id: a unique, meaningful id for the task
    :param owner: the owner of the task. Using a meaningful description
        (e.g. user/person/team/role name) to clarify ownership is recommended.
    :param email: the 'to' email address(es) used in email alerts. This can be a
        single email or multiple ones. Multiple addresses can be specified as a
        comma or semicolon separated string or by passing a list of strings.
    :param email_on_retry: Indicates whether email alerts should be sent when a
        task is retried
    :param email_on_failure: Indicates whether email alerts should be sent when
        a task failed
    :param retries: the number of retries that should be performed before
        failing the task
    :param retry_delay: delay between retries, can be set as ``timedelta`` or
        ``float`` seconds, which will be converted into ``timedelta``,
        the default is ``timedelta(seconds=300)``.
    :param retry_exponential_backoff: allow progressively longer waits between
        retries by using exponential backoff algorithm on retry delay (delay
        will be converted into seconds)
    :param max_retry_delay: maximum delay interval between retries, can be set as
        ``timedelta`` or ``float`` seconds, which will be converted into ``timedelta``.
    :param start_date: The ``start_date`` for the task, determines
        the ``logical_date`` for the first task instance. The best practice
        is to have the start_date rounded
        to your DAG's schedule. Daily jobs have their start_date
        some day at 00:00:00, hourly jobs have their start_date at 00:00
        of a specific hour. Note that Airflow simply looks at the latest
        ``logical_date`` and adds the schedule to determine
        the next ``logical_date``. It is also very important
        to note that different tasks' dependencies
        need to line up in time. If task A depends on task B and their
        start_date are offset in a way that their logical_date don't line
        up, A's dependencies will never be met. If you are looking to delay
        a task, for example running a daily task at 2AM, look into the
        ``TimeSensor`` and ``TimeDeltaSensor``. We advise against using
        dynamic ``start_date`` and recommend using fixed ones. Read the
        FAQ entry about start_date for more information.
    :param end_date: if specified, the scheduler won't go beyond this date
    :param depends_on_past: when set to true, task instances will run
        sequentially and only if the previous instance has succeeded or has been skipped.
        The task instance for the start_date is allowed to run.
    :param wait_for_past_depends_before_skipping: when set to true, if the task instance
        should be marked as skipped, and depends_on_past is true, the ti will stay on None state
        waiting the task of the previous run
    :param wait_for_downstream: when set to true, an instance of task
        X will wait for tasks immediately downstream of the previous instance
        of task X to finish successfully or be skipped before it runs. This is useful if the
        different instances of a task X alter the same asset, and this asset
        is used by tasks downstream of task X. Note that depends_on_past
        is forced to True wherever wait_for_downstream is used. Also note that
        only tasks *immediately* downstream of the previous task instance are waited
        for; the statuses of any tasks further downstream are ignored.
    :param dag: a reference to the dag the task is attached to (if any)
    :param priority_weight: priority weight of this task against other task.
        This allows the executor to trigger higher priority tasks before
        others when things get backed up. Set priority_weight as a higher
        number for more important tasks.
        As not all database engines support 64-bit integers, values are capped with 32-bit.
        Valid range is from -2,147,483,648 to 2,147,483,647.
    :param weight_rule: weighting method used for the effective total
        priority weight of the task. Options are:
        ``{ downstream | upstream | absolute }`` default is ``downstream``
        When set to ``downstream`` the effective weight of the task is the
        aggregate sum of all downstream descendants. As a result, upstream
        tasks will have higher weight and will be scheduled more aggressively
        when using positive weight values. This is useful when you have
        multiple dag run instances and desire to have all upstream tasks to
        complete for all runs before each dag can continue processing
        downstream tasks. When set to ``upstream`` the effective weight is the
        aggregate sum of all upstream ancestors. This is the opposite where
        downstream tasks have higher weight and will be scheduled more
        aggressively when using positive weight values. This is useful when you
        have multiple dag run instances and prefer to have each dag complete
        before starting upstream tasks of other dags.  When set to
        ``absolute``, the effective weight is the exact ``priority_weight``
        specified without additional weighting. You may want to do this when
        you know exactly what priority weight each task should have.
        Additionally, when set to ``absolute``, there is bonus effect of
        significantly speeding up the task creation process as for very large
        DAGs. Options can be set as string or using the constants defined in
        the static class ``airflow.utils.WeightRule``.
        Irrespective of the weight rule, resulting priority values are capped with 32-bit.
        |experimental|
        Since 2.9.0, Airflow allows to define custom priority weight strategy,
        by creating a subclass of
        ``airflow.task.priority_strategy.PriorityWeightStrategy`` and registering
        in a plugin, then providing the class path or the class instance via
        ``weight_rule`` parameter. The custom priority weight strategy will be
        used to calculate the effective total priority weight of the task instance.
    :param queue: which queue to target when running this job. Not
        all executors implement queue management, the CeleryExecutor
        does support targeting specific queues.
    :param pool: the slot pool this task should run in, slot pools are a
        way to limit concurrency for certain tasks
    :param pool_slots: the number of pool slots this task should use (>= 1)
        Values less than 1 are not allowed.
    :param sla: DEPRECATED - The SLA feature is removed in Airflow 3.0, to be replaced with a new implementation in 3.1
    :param execution_timeout: max time allowed for the execution of
        this task instance, if it goes beyond it will raise and fail.
    :param on_failure_callback: a function or list of functions to be called when a task instance
        of this task fails. a context dictionary is passed as a single
        parameter to this function. Context contains references to related
        objects to the task instance and is documented under the macros
        section of the API.
    :param on_execute_callback: much like the ``on_failure_callback`` except
        that it is executed right before the task is executed.
    :param on_retry_callback: much like the ``on_failure_callback`` except
        that it is executed when retries occur.
    :param on_success_callback: much like the ``on_failure_callback`` except
        that it is executed when the task succeeds.
    :param on_skipped_callback: much like the ``on_failure_callback`` except
        that it is executed when skipped occur; this callback will be called only if AirflowSkipException get raised.
        Explicitly it is NOT called if a task is not started to be executed because of a preceding branching
        decision in the DAG or a trigger rule which causes execution to skip so that the task execution
        is never scheduled.
    :param pre_execute: a function to be called immediately before task
        execution, receiving a context dictionary; raising an exception will
        prevent the task from being executed.

        |experimental|
    :param post_execute: a function to be called immediately after task
        execution, receiving a context dictionary and task result; raising an
        exception will prevent the task from succeeding.

        |experimental|
    :param trigger_rule: defines the rule by which dependencies are applied
        for the task to get triggered. Options are:
        ``{ all_success | all_failed | all_done | all_skipped | one_success | one_done |
        one_failed | none_failed | none_failed_min_one_success | none_skipped | always}``
        default is ``all_success``. Options can be set as string or
        using the constants defined in the static class
        ``airflow.utils.TriggerRule``
    :param resources: A map of resource parameter names (the argument names of the
        Resources constructor) to their values.
    :param run_as_user: unix username to impersonate while running the task
    :param max_active_tis_per_dag: When set, a task will be able to limit the concurrent
        runs across logical_dates.
    :param max_active_tis_per_dagrun: When set, a task will be able to limit the concurrent
        task instances per DAG run.
    :param executor: Which executor to target when running this task. NOT YET SUPPORTED
    :param executor_config: Additional task-level configuration parameters that are
        interpreted by a specific executor. Parameters are namespaced by the name of
        executor.

        **Example**: to run this task in a specific docker container through
        the KubernetesExecutor ::

            MyOperator(..., executor_config={"KubernetesExecutor": {"image": "myCustomDockerImage"}})

    :param do_xcom_push: if True, an XCom is pushed containing the Operator's
        result
    :param multiple_outputs: if True and do_xcom_push is True, pushes multiple XComs, one for each
        key in the returned dictionary result. If False and do_xcom_push is True, pushes a single XCom.
    :param task_group: The TaskGroup to which the task should belong. This is typically provided when not
        using a TaskGroup as a context manager.
    :param doc: Add documentation or notes to your Task objects that is visible in
        Task Instance details View in the Webserver
    :param doc_md: Add documentation (in Markdown format) or notes to your Task objects
        that is visible in Task Instance details View in the Webserver
    :param doc_rst: Add documentation (in RST format) or notes to your Task objects
        that is visible in Task Instance details View in the Webserver
    :param doc_json: Add documentation (in JSON format) or notes to your Task objects
        that is visible in Task Instance details View in the Webserver
    :param doc_yaml: Add documentation (in YAML format) or notes to your Task objects
        that is visible in Task Instance details View in the Webserver
    :param task_display_name: The display name of the task which appears on the UI.
    :param logger_name: Name of the logger used by the Operator to emit logs.
        If set to `None` (default), the logger name will fall back to
        `airflow.task.operators.{class.__module__}.{class.__name__}` (e.g. SimpleHttpOperator will have
        *airflow.task.operators.airflow.providers.http.operators.http.SimpleHttpOperator* as logger).
    :param allow_nested_operators: if True, when an operator is executed within another one a warning message
        will be logged. If False, then an exception will be raised if the operator is badly used (e.g. nested
        within another one). In future releases of Airflow this parameter will be removed and an exception
        will always be thrown when operators are nested within each other (default is True).

        **Example**: example of a bad operator mixin usage::

            @task(provide_context=True)
            def say_hello_world(**context):
                hello_world_task = BashOperator(
                    task_id="hello_world_task",
                    bash_command="python -c \"print('Hello, world!')\"",
                    dag=dag,
                )
                hello_world_task.execute(context)
    """

    start_trigger_args: StartTriggerArgs | None = None
    start_from_trigger: bool = False

    on_execute_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None
    on_failure_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None
    on_success_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None
    on_retry_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None
    on_skipped_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None

    def __init__(
        self,
        pre_execute=None,
        post_execute=None,
        on_execute_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        on_failure_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        on_success_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        on_retry_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        on_skipped_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        **kwargs,
    ):
        if start_date := kwargs.get("start_date", None):
            kwargs["start_date"] = timezone.convert_to_utc(start_date)

        if end_date := kwargs.get("end_date", None):
            kwargs["end_date"] = timezone.convert_to_utc(end_date)
        super().__init__(**kwargs)
        self._pre_execute_hook = pre_execute
        self._post_execute_hook = post_execute
        self.on_execute_callback = on_execute_callback
        self.on_failure_callback = on_failure_callback
        self.on_success_callback = on_success_callback
        self.on_skipped_callback = on_skipped_callback
        self.on_retry_callback = on_retry_callback

    # Defines the operator level extra links
    operator_extra_links: Collection[BaseOperatorLink] = ()

    if TYPE_CHECKING:

        @property  # type: ignore[override]
        def dag(self) -> SchedulerDAG:  # type: ignore[override]
            return super().dag  # type: ignore[return-value]

        @dag.setter
        def dag(self, val: SchedulerDAG):
            # For type checking only
            ...

    @classmethod
    @methodtools.lru_cache(maxsize=None)
    def get_serialized_fields(cls):
        """Stringified DAGs and operators contain exactly these fields."""
        # TODO: this ends up caching it once per-subclass, which isn't what we want, but this class is only
        # kept around during the development of AIP-72/TaskSDK code.
        return TaskSDKBaseOperator.get_serialized_fields() | {
            "start_trigger_args",
            "start_from_trigger",
            "on_execute_callback",
            "on_failure_callback",
            "on_success_callback",
            "on_retry_callback",
            "on_skipped_callback",
        }

    def get_inlet_defs(self):
        """
        Get inlet definitions on this task.

        :meta private:
        """
        return self.inlets

    def get_outlet_defs(self):
        """
        Get outlet definitions on this task.

        :meta private:
        """
        return self.outlets

    deps: frozenset[BaseTIDep] = frozenset(
        {
            NotInRetryPeriodDep(),
            PrevDagrunDep(),
            TriggerRuleDep(),
            NotPreviouslySkippedDep(),
            MappedTaskUpstreamDep(),
        }
    )
    """
    Returns the set of dependencies for the operator. These differ from execution
    context dependencies in that they are specific to tasks and can be
    extended/overridden by subclasses.
    """

    @prepare_lineage
    def pre_execute(self, context: Any):
        """Execute right before self.execute() is called."""
        if self._pre_execute_hook is None:
            return
        ExecutionCallableRunner(
            self._pre_execute_hook,
            context_get_outlet_events(context),
            logger=self.log,
        ).run(context)

    def execute(self, context: Context) -> Any:
        """
        Derive when creating an operator.

        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        raise NotImplementedError()

    @apply_lineage
    def post_execute(self, context: Any, result: Any = None):
        """
        Execute right after self.execute() is called.

        It is passed the execution context and any results returned by the operator.
        """
        if self._post_execute_hook is None:
            return
        ExecutionCallableRunner(
            self._post_execute_hook,
            context_get_outlet_events(context),
            logger=self.log,
        ).run(context, result)

    @provide_session
    def clear(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        upstream: bool = False,
        downstream: bool = False,
        session: Session = NEW_SESSION,
    ):
        """Clear the state of task instances associated with the task, following the parameters specified."""
        qry = select(TaskInstance).where(TaskInstance.dag_id == self.dag_id)

        if start_date:
            qry = qry.where(TaskInstance.logical_date >= start_date)
        if end_date:
            qry = qry.where(TaskInstance.logical_date <= end_date)

        tasks = [self.task_id]

        if upstream:
            tasks += [t.task_id for t in self.get_flat_relatives(upstream=True)]

        if downstream:
            tasks += [t.task_id for t in self.get_flat_relatives(upstream=False)]

        qry = qry.where(TaskInstance.task_id.in_(tasks))
        results = session.scalars(qry).all()
        count = len(results)

        if TYPE_CHECKING:
            # TODO: Task-SDK: We need to set this to the scheduler DAG until we fully separate scheduling and
            # definition code
            assert isinstance(self.dag, SchedulerDAG)

        clear_task_instances(results, session, dag=self.dag)
        session.commit()
        return count

    @provide_session
    def get_task_instances(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        session: Session = NEW_SESSION,
    ) -> list[TaskInstance]:
        """Get task instances related to this task for a specific date range."""
        from airflow.models import DagRun

        query = (
            select(TaskInstance)
            .join(TaskInstance.dag_run)
            .where(TaskInstance.dag_id == self.dag_id)
            .where(TaskInstance.task_id == self.task_id)
        )
        if start_date:
            query = query.where(DagRun.logical_date >= start_date)
        if end_date:
            query = query.where(DagRun.logical_date <= end_date)
        return session.scalars(query.order_by(DagRun.logical_date)).all()

    @provide_session
    def run(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        ignore_first_depends_on_past: bool = True,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        session: Session = NEW_SESSION,
    ) -> None:
        """Run a set of task instances for a date range."""
        from airflow.models import DagRun
        from airflow.utils.types import DagRunType

        # Assertions for typing -- we need a dag, for this function, and when we have a DAG we are
        # _guaranteed_ to have start_date (else we couldn't have been added to a DAG)
        if TYPE_CHECKING:
            assert self.start_date

            # TODO: Task-SDK: We need to set this to the scheduler DAG until we fully separate scheduling and
            # definition code
            assert isinstance(self.dag, SchedulerDAG)

        start_date = pendulum.instance(start_date or self.start_date)
        end_date = pendulum.instance(end_date or self.end_date or timezone.utcnow())

        for info in self.dag.iter_dagrun_infos_between(start_date, end_date, align=False):
            ignore_depends_on_past = info.logical_date == start_date and ignore_first_depends_on_past
            try:
                dag_run = session.scalars(
                    select(DagRun).where(
                        DagRun.dag_id == self.dag_id,
                        DagRun.logical_date == info.logical_date,
                    )
                ).one()
                ti = TaskInstance(self, run_id=dag_run.run_id)
            except NoResultFound:
                # This is _mostly_ only used in tests
                dr = DagRun(
                    dag_id=self.dag_id,
                    run_id=DagRun.generate_run_id(DagRunType.MANUAL, info.logical_date),
                    run_type=DagRunType.MANUAL,
                    logical_date=info.logical_date,
                    data_interval=info.data_interval,
                    run_after=info.run_after,
                    triggered_by=DagRunTriggeredByType.TEST,
                    state=DagRunState.RUNNING,
                )
                ti = TaskInstance(self, run_id=dr.run_id)
                ti.dag_run = dr
                session.add(dr)
                session.flush()

            ti.run(
                mark_success=mark_success,
                ignore_depends_on_past=ignore_depends_on_past,
                wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
                ignore_ti_state=ignore_ti_state,
                test_mode=test_mode,
                session=session,
            )

    def dry_run(self) -> None:
        """Perform dry run for the operator - just render template fields."""
        self.log.info("Dry run")
        for field in self.template_fields:
            try:
                content = getattr(self, field)
            except AttributeError:
                raise AttributeError(
                    f"{field!r} is configured as a template field "
                    f"but {self.task_type} does not have this attribute."
                )

            if content and isinstance(content, str):
                self.log.info("Rendering template for %s", field)
                self.log.info(content)

    def get_direct_relatives(self, upstream: bool = False) -> Iterable[Operator]:
        """Get list of the direct relatives to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    @staticmethod
    def xcom_push(
        context: Any,
        key: str,
        value: Any,
    ) -> None:
        """
        Make an XCom available for tasks to pull.

        :param context: Execution Context Dictionary
        :param key: A key for the XCom
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        """
        context["ti"].xcom_push(key=key, value=value)

    @staticmethod
    @provide_session
    def xcom_pull(
        context: Any,
        task_ids: str | list[str] | None = None,
        dag_id: str | None = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool | None = None,
        session: Session = NEW_SESSION,
    ) -> Any:
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param context: Execution Context Dictionary
        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :param include_prior_dates: If False, only XComs from the current
            logical_date are returned. If True, XComs from previous dates
            are returned as well.
        """
        return context["ti"].xcom_pull(
            key=key,
            task_ids=task_ids,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates,
            session=session,
        )

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Serialize; required by DAGNode."""
        return DagAttributeTypes.OP, self.task_id

    def defer(
        self,
        *,
        trigger: BaseTrigger,
        method_name: str,
        kwargs: dict[str, Any] | None = None,
        timeout: timedelta | int | float | None = None,
    ) -> NoReturn:
        """
        Mark this Operator "deferred", suspending its execution until the provided trigger fires an event.

        This is achieved by raising a special exception (TaskDeferred)
        which is caught in the main _execute_task wrapper. Triggers can send execution back to task or end
        the task instance directly. If the trigger will end the task instance itself, ``method_name`` should
        be None; otherwise, provide the name of the method that should be used when resuming execution in
        the task.
        """
        raise TaskDeferred(trigger=trigger, method_name=method_name, kwargs=kwargs, timeout=timeout)

    def next_callable(self, next_method, next_kwargs) -> Callable[..., Any]:
        """Get the next callable from given operator."""
        # __fail__ is a special signal value for next_method that indicates
        # this task was scheduled specifically to fail.
        if next_method == TRIGGER_FAIL_REPR:
            next_kwargs = next_kwargs or {}
            traceback = next_kwargs.get("traceback")
            if traceback is not None:
                self.log.error("Trigger failed:\n%s", "\n".join(traceback))
            if (error := next_kwargs.get("error", "Unknown")) == TriggerFailureReason.TRIGGER_TIMEOUT:
                raise TaskDeferralTimeout(error)
            else:
                raise TaskDeferralError(error)
        # Grab the callable off the Operator/Task and add in any kwargs
        execute_callable = getattr(self, next_method)
        if next_kwargs:
            execute_callable = functools.partial(execute_callable, **next_kwargs)
        return execute_callable

    def resume_execution(self, next_method: str, next_kwargs: dict[str, Any] | None, context: Context):
        """Call this method when a deferred task is resumed."""
        execute_callable = self.next_callable(self, next_method, next_kwargs)
        return execute_callable(context)

    def unmap(self, resolve: None | dict[str, Any] | tuple[Context, Session]) -> BaseOperator:
        """
        Get the "normal" operator from the current operator.

        Since a BaseOperator is not mapped to begin with, this simply returns
        the original operator.

        :meta private:
        """
        return self

    def expand_start_from_trigger(self, *, context: Context, session: Session) -> bool:
        """
        Get the start_from_trigger value of the current abstract operator.

        Since a BaseOperator is not mapped to begin with, this simply returns
        the original value of start_from_trigger.

        :meta private:
        """
        return self.start_from_trigger

    def expand_start_trigger_args(self, *, context: Context, session: Session) -> StartTriggerArgs | None:
        """
        Get the start_trigger_args value of the current abstract operator.

        Since a BaseOperator is not mapped to begin with, this simply returns
        the original value of start_trigger_args.

        :meta private:
        """
        return self.start_trigger_args

    if TYPE_CHECKING:

        @classmethod
        def get_mapped_ti_count(
            cls, node: DAGNode | MappedTaskGroup, run_id: str, *, session: Session
        ) -> int:
            """
            Return the number of mapped TaskInstances that can be created at run time.

            This considers both literal and non-literal mapped arguments, and the
            result is therefore available when all depended tasks have finished. The
            return value should be identical to ``parse_time_mapped_ti_count`` if
            all mapped arguments are literal.

            :raise NotFullyPopulated: If upstream tasks are not all complete yet.
            :raise NotMapped: If the operator is neither mapped, nor has any parent
                mapped task groups.
            :return: Total number of mapped TIs this task should have.
            """
    else:

        @singledispatchmethod
        @classmethod
        def get_mapped_ti_count(cls, task: DAGNode, run_id: str, *, session: Session) -> int:
            raise NotImplementedError(f"Not implemented for {type(task)}")

        # https://github.com/python/cpython/issues/86153
        # WHile we support Python 3.9 we can't rely on the type hint, we need to pass the type explicitly to
        # register.
        @get_mapped_ti_count.register(TaskSDKAbstractOperator)
        @classmethod
        def _(cls, task: TaskSDKAbstractOperator, run_id: str, *, session: Session) -> int:
            group = task.get_closest_mapped_task_group()
            if group is None:
                raise NotMapped()
            return cls.get_mapped_ti_count(group, run_id, session=session)

        @get_mapped_ti_count.register(MappedOperator)
        @classmethod
        def _(cls, task: MappedOperator, run_id: str, *, session: Session) -> int:
            from airflow.serialization.serialized_objects import _ExpandInputRef

            exp_input = task._get_specified_expand_input()
            if isinstance(exp_input, _ExpandInputRef):
                exp_input = exp_input.deref(task.dag)
            current_count = exp_input.get_total_map_length(run_id, session=session)

            group = task.get_closest_mapped_task_group()
            if group is None:
                return current_count
            parent_count = cls.get_mapped_ti_count(group, run_id, session=session)
            return parent_count * current_count

        @get_mapped_ti_count.register(TaskGroup)
        @classmethod
        def _(cls, group: TaskGroup, run_id: str, *, session: Session) -> int:
            """
            Return the number of instances a task in this group should be mapped to at run time.

            This considers both literal and non-literal mapped arguments, and the
            result is therefore available when all depended tasks have finished. The
            return value should be identical to ``parse_time_mapped_ti_count`` if
            all mapped arguments are literal.

            If this group is inside mapped task groups, all the nested counts are
            multiplied and accounted.

            :raise NotFullyPopulated: If upstream tasks are not all complete yet.
            :return: Total number of mapped TIs this task should have.
            """

            def iter_mapped_task_groups(group) -> Iterator[MappedTaskGroup]:
                while group is not None:
                    if isinstance(group, MappedTaskGroup):
                        yield group
                    group = group.parent_group

            groups = iter_mapped_task_groups(group)
            return functools.reduce(
                operator.mul,
                (g._expand_input.get_total_map_length(run_id, session=session) for g in groups),
            )


def chain(*tasks: DependencyMixin | Sequence[DependencyMixin]) -> None:
    r"""
    Given a number of tasks, builds a dependency chain.

    This function accepts values of BaseOperator (aka tasks), EdgeModifiers (aka Labels), XComArg, TaskGroups,
    or lists containing any mix of these types (or a mix in the same list). If you want to chain between two
    lists you must ensure they have the same length.

    Using classic operators/sensors:

    .. code-block:: python

        chain(t1, [t2, t3], [t4, t5], t6)

    is equivalent to::

          / -> t2 -> t4 \
        t1               -> t6
          \ -> t3 -> t5 /

    .. code-block:: python

        t1.set_downstream(t2)
        t1.set_downstream(t3)
        t2.set_downstream(t4)
        t3.set_downstream(t5)
        t4.set_downstream(t6)
        t5.set_downstream(t6)

    Using task-decorated functions aka XComArgs:

    .. code-block:: python

        chain(x1(), [x2(), x3()], [x4(), x5()], x6())

    is equivalent to::

          / -> x2 -> x4 \
        x1               -> x6
          \ -> x3 -> x5 /

    .. code-block:: python

        x1 = x1()
        x2 = x2()
        x3 = x3()
        x4 = x4()
        x5 = x5()
        x6 = x6()
        x1.set_downstream(x2)
        x1.set_downstream(x3)
        x2.set_downstream(x4)
        x3.set_downstream(x5)
        x4.set_downstream(x6)
        x5.set_downstream(x6)

    Using TaskGroups:

    .. code-block:: python

        chain(t1, task_group1, task_group2, t2)

        t1.set_downstream(task_group1)
        task_group1.set_downstream(task_group2)
        task_group2.set_downstream(t2)


    It is also possible to mix between classic operator/sensor, EdgeModifiers, XComArg, and TaskGroups:

    .. code-block:: python

        chain(t1, [Label("branch one"), Label("branch two")], [x1(), x2()], task_group1, x3())

    is equivalent to::

          / "branch one" -> x1 \
        t1                      -> task_group1 -> x3
          \ "branch two" -> x2 /

    .. code-block:: python

        x1 = x1()
        x2 = x2()
        x3 = x3()
        label1 = Label("branch one")
        label2 = Label("branch two")
        t1.set_downstream(label1)
        label1.set_downstream(x1)
        t2.set_downstream(label2)
        label2.set_downstream(x2)
        x1.set_downstream(task_group1)
        x2.set_downstream(task_group1)
        task_group1.set_downstream(x3)

        # or

        x1 = x1()
        x2 = x2()
        x3 = x3()
        t1.set_downstream(x1, edge_modifier=Label("branch one"))
        t1.set_downstream(x2, edge_modifier=Label("branch two"))
        x1.set_downstream(task_group1)
        x2.set_downstream(task_group1)
        task_group1.set_downstream(x3)


    :param tasks: Individual and/or list of tasks, EdgeModifiers, XComArgs, or TaskGroups to set dependencies
    """
    for up_task, down_task in zip(tasks, tasks[1:]):
        if isinstance(up_task, DependencyMixin):
            up_task.set_downstream(down_task)
            continue
        if isinstance(down_task, DependencyMixin):
            down_task.set_upstream(up_task)
            continue
        if not isinstance(up_task, Sequence) or not isinstance(down_task, Sequence):
            raise TypeError(f"Chain not supported between instances of {type(up_task)} and {type(down_task)}")
        up_task_list = up_task
        down_task_list = down_task
        if len(up_task_list) != len(down_task_list):
            raise AirflowException(
                f"Chain not supported for different length Iterable. "
                f"Got {len(up_task_list)} and {len(down_task_list)}."
            )
        for up_t, down_t in zip(up_task_list, down_task_list):
            up_t.set_downstream(down_t)


def cross_downstream(
    from_tasks: Sequence[DependencyMixin],
    to_tasks: DependencyMixin | Sequence[DependencyMixin],
):
    r"""
    Set downstream dependencies for all tasks in from_tasks to all tasks in to_tasks.

    Using classic operators/sensors:

    .. code-block:: python

        cross_downstream(from_tasks=[t1, t2, t3], to_tasks=[t4, t5, t6])

    is equivalent to::

        t1 ---> t4
           \ /
        t2 -X -> t5
           / \
        t3 ---> t6

    .. code-block:: python

        t1.set_downstream(t4)
        t1.set_downstream(t5)
        t1.set_downstream(t6)
        t2.set_downstream(t4)
        t2.set_downstream(t5)
        t2.set_downstream(t6)
        t3.set_downstream(t4)
        t3.set_downstream(t5)
        t3.set_downstream(t6)

    Using task-decorated functions aka XComArgs:

    .. code-block:: python

        cross_downstream(from_tasks=[x1(), x2(), x3()], to_tasks=[x4(), x5(), x6()])

    is equivalent to::

        x1 ---> x4
           \ /
        x2 -X -> x5
           / \
        x3 ---> x6

    .. code-block:: python

        x1 = x1()
        x2 = x2()
        x3 = x3()
        x4 = x4()
        x5 = x5()
        x6 = x6()
        x1.set_downstream(x4)
        x1.set_downstream(x5)
        x1.set_downstream(x6)
        x2.set_downstream(x4)
        x2.set_downstream(x5)
        x2.set_downstream(x6)
        x3.set_downstream(x4)
        x3.set_downstream(x5)
        x3.set_downstream(x6)

    It is also possible to mix between classic operator/sensor and XComArg tasks:

    .. code-block:: python

        cross_downstream(from_tasks=[t1, x2(), t3], to_tasks=[x1(), t2, x3()])

    is equivalent to::

        t1 ---> x1
           \ /
        x2 -X -> t2
           / \
        t3 ---> x3

    .. code-block:: python

        x1 = x1()
        x2 = x2()
        x3 = x3()
        t1.set_downstream(x1)
        t1.set_downstream(t2)
        t1.set_downstream(x3)
        x2.set_downstream(x1)
        x2.set_downstream(t2)
        x2.set_downstream(x3)
        t3.set_downstream(x1)
        t3.set_downstream(t2)
        t3.set_downstream(x3)

    :param from_tasks: List of tasks or XComArgs to start from.
    :param to_tasks: List of tasks or XComArgs to set as downstream dependencies.
    """
    for task in from_tasks:
        task.set_downstream(to_tasks)


def chain_linear(*elements: DependencyMixin | Sequence[DependencyMixin]):
    """
    Simplify task dependency definition.

    E.g.: suppose you want precedence like so::

            ╭─op2─╮ ╭─op4─╮
        op1─┤     ├─├─op5─┤─op7
            ╰-op3─╯ ╰-op6─╯

    Then you can accomplish like so::

        chain_linear(op1, [op2, op3], [op4, op5, op6], op7)

    :param elements: a list of operators / lists of operators
    """
    if not elements:
        raise ValueError("No tasks provided; nothing to do.")
    prev_elem = None
    deps_set = False
    for curr_elem in elements:
        if isinstance(curr_elem, (EdgeModifier, TaskSDKEdgeModifier)):
            raise ValueError("Labels are not supported by chain_linear")
        if prev_elem is not None:
            for task in prev_elem:
                task >> curr_elem
                if not deps_set:
                    deps_set = True
        prev_elem = [curr_elem] if isinstance(curr_elem, DependencyMixin) else curr_elem
    if not deps_set:
        raise ValueError("No dependencies were set. Did you forget to expand with `*`?")
