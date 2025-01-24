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
from __future__ import annotations

from collections.abc import Iterable, Sequence
from types import GeneratorType
from typing import TYPE_CHECKING

from sqlalchemy import tuple_, update

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.operator import Operator
    from airflow.sdk.definitions._internal.node import DAGNode

# The key used by SkipMixin to store XCom data.
XCOM_SKIPMIXIN_KEY = "skipmixin_key"

# The dictionary key used to denote task IDs that are skipped
XCOM_SKIPMIXIN_SKIPPED = "skipped"

# The dictionary key used to denote task IDs that are followed
XCOM_SKIPMIXIN_FOLLOWED = "followed"


def _ensure_tasks(nodes: Iterable[DAGNode]) -> Sequence[Operator]:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.mappedoperator import MappedOperator

    return [n for n in nodes if isinstance(n, (BaseOperator, MappedOperator))]


class SkipMixin(LoggingMixin):
    """A Mixin to skip Tasks Instances."""

    @staticmethod
    def _set_state_to_skipped(
        dag_id: str,
        run_id: str,
        tasks: Sequence[str] | Sequence[tuple[str, int]],
        session: Session,
    ) -> None:
        """Set state of task instances to skipped from the same dag run."""
        if tasks:
            now = timezone.utcnow()

            if isinstance(tasks[0], tuple):
                session.execute(
                    update(TaskInstance)
                    .where(
                        TaskInstance.dag_id == dag_id,
                        TaskInstance.run_id == run_id,
                        tuple_(TaskInstance.task_id, TaskInstance.map_index).in_(tasks),
                    )
                    .values(state=TaskInstanceState.SKIPPED, start_date=now, end_date=now)
                    .execution_options(synchronize_session=False)
                )
            else:
                session.execute(
                    update(TaskInstance)
                    .where(
                        TaskInstance.dag_id == dag_id,
                        TaskInstance.run_id == run_id,
                        TaskInstance.task_id.in_(tasks),
                    )
                    .values(state=TaskInstanceState.SKIPPED, start_date=now, end_date=now)
                    .execution_options(synchronize_session=False)
                )

    @provide_session
    def skip(
        self,
        dag_id: str,
        run_id: str,
        tasks: Iterable[DAGNode],
        map_index: int = -1,
        session: Session = NEW_SESSION,
    ):
        """
        Set tasks instances to skipped from the same dag run.

        If this instance has a `task_id` attribute, store the list of skipped task IDs to XCom
        so that NotPreviouslySkippedDep knows these tasks should be skipped when they
        are cleared.

        :param dag_id: the dag_id of the dag run for which to set the tasks to skipped
        :param run_id: the run_id of the dag run for which to set the tasks to skipped
        :param tasks: tasks to skip (not task_ids)
        :param session: db session to use
        :param map_index: map_index of the current task instance
        """
        # SkipMixin may not necessarily have a task_id attribute. Only store to XCom if one is available.
        task_id: str | None = getattr(self, "task_id", None)
        task_list = _ensure_tasks(tasks)
        if not task_list:
            return

        task_ids_list = [d.task_id for d in task_list]
        SkipMixin._set_state_to_skipped(dag_id, run_id, task_ids_list, session)
        session.commit()

        if task_id is not None:
            from airflow.models.xcom import XCom

            XCom.set(
                key=XCOM_SKIPMIXIN_KEY,
                value={XCOM_SKIPMIXIN_SKIPPED: task_ids_list},
                task_id=task_id,
                dag_id=dag_id,
                run_id=run_id,
                map_index=map_index,
                session=session,
            )

    def skip_all_except(
        self,
        ti: TaskInstance,
        branch_task_ids: None | str | Iterable[str],
    ):
        """
        Implement the logic for a branching operator.

        Given a single task ID or list of task IDs to follow, this skips all other tasks
        immediately downstream of this operator.

        branch_task_ids is stored to XCom so that NotPreviouslySkippedDep knows skipped tasks or
        newly added tasks should be skipped when they are cleared.
        """
        # Ensure we don't serialize a generator object
        if branch_task_ids and isinstance(branch_task_ids, GeneratorType):
            branch_task_ids = list(branch_task_ids)
        log = self.log  # Note: need to catch logger form instance, static logger breaks pytest
        if isinstance(branch_task_ids, str):
            branch_task_id_set = {branch_task_ids}
        elif isinstance(branch_task_ids, Iterable):
            branch_task_id_set = set(branch_task_ids)
            invalid_task_ids_type = {
                (bti, type(bti).__name__) for bti in branch_task_id_set if not isinstance(bti, str)
            }
            if invalid_task_ids_type:
                raise AirflowException(
                    f"'branch_task_ids' expected all task IDs are strings. "
                    f"Invalid tasks found: {invalid_task_ids_type}."
                )
        elif branch_task_ids is None:
            branch_task_id_set = set()
        else:
            raise AirflowException(
                "'branch_task_ids' must be either None, a task ID, or an Iterable of IDs, "
                f"but got {type(branch_task_ids).__name__!r}."
            )

        log.info("Following branch %s", branch_task_id_set)

        if TYPE_CHECKING:
            assert ti.task

        task = ti.task
        session = settings.Session()
        dag = TaskInstance.ensure_dag(ti, session=session)

        valid_task_ids = set(dag.task_ids)
        invalid_task_ids = branch_task_id_set - valid_task_ids
        if invalid_task_ids:
            raise AirflowException(
                "'branch_task_ids' must contain only valid task_ids. "
                f"Invalid tasks found: {invalid_task_ids}."
            )

        downstream_tasks = _ensure_tasks(task.downstream_list)

        if downstream_tasks:
            # For a branching workflow that looks like this, when "branch" does skip_all_except("task1"),
            # we intuitively expect both "task1" and "join" to execute even though strictly speaking,
            # "join" is also immediately downstream of "branch" and should have been skipped. Therefore,
            # we need a special case here for such empty branches: Check downstream tasks of branch_task_ids.
            # In case the task to skip is also downstream of branch_task_ids, we add it to branch_task_ids and
            # exclude it from skipping.
            #
            # branch  ----->  join
            #   \            ^
            #     v        /
            #       task1
            #
            for branch_task_id in list(branch_task_id_set):
                branch_task_id_set.update(dag.get_task(branch_task_id).get_flat_relative_ids(upstream=False))

            dag_run = ti.get_dagrun(session=session)
            skip_tasks = [
                (t.task_id, downstream_ti.map_index)
                for t in downstream_tasks
                if (
                    downstream_ti := dag_run.get_task_instance(
                        t.task_id, map_index=ti.map_index, session=session
                    )
                )
                and t.task_id not in branch_task_id_set
            ]

            follow_task_ids = [t.task_id for t in downstream_tasks if t.task_id in branch_task_id_set]
            log.info("Skipping tasks %s", skip_tasks)
            SkipMixin._set_state_to_skipped(dag_run.dag_id, dag_run.run_id, skip_tasks, session=session)
            ti.xcom_push(
                key=XCOM_SKIPMIXIN_KEY, value={XCOM_SKIPMIXIN_FOLLOWED: follow_task_ids}, session=session
            )
