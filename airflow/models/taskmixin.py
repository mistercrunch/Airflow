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

import warnings
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Iterable, Sequence

import pendulum

from airflow.exceptions import AirflowException, RemovedInAirflow3Warning
from airflow.serialization.enums import DagAttributeTypes
from airflow.utils.setup_teardown import SetupTeardownContext
from airflow.utils.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from logging import Logger

    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG
    from airflow.models.operator import Operator
    from airflow.utils.edgemodifier import EdgeModifier
    from airflow.utils.task_group import TaskGroup


class DependencyMixin:
    """Mixing implementing common dependency setting methods methods like >> and <<."""

    @property
    def roots(self) -> Sequence[DependencyMixin]:
        """
        List of root nodes -- ones with no upstream dependencies.

        a.k.a. the "start" of this sub-graph
        """
        raise NotImplementedError()

    @property
    def leaves(self) -> Sequence[DependencyMixin]:
        """
        List of leaf nodes -- ones with only upstream dependencies.

        a.k.a. the "end" of this sub-graph
        """
        raise NotImplementedError()

    @abstractmethod
    def set_upstream(
        self, other: DependencyMixin | Sequence[DependencyMixin], edge_modifier: EdgeModifier | None = None
    ):
        """Set a task or a task list to be directly upstream from the current task."""
        raise NotImplementedError()

    @abstractmethod
    def set_downstream(
        self, other: DependencyMixin | Sequence[DependencyMixin], edge_modifier: EdgeModifier | None = None
    ):
        """Set a task or a task list to be directly downstream from the current task."""
        raise NotImplementedError()

    def as_setup(self) -> DependencyMixin:
        """Mark a task as setup task."""
        raise NotImplementedError()

    def as_teardown(
        self,
        *,
        setups: BaseOperator | Iterable[BaseOperator] | ArgNotSet = NOTSET,
        on_failure_fail_dagrun=NOTSET,
    ) -> DependencyMixin:
        """Mark a task as teardown and set its setups as direct relatives."""
        raise NotImplementedError()

    def update_relative(
        self, other: DependencyMixin, upstream: bool = True, edge_modifier: EdgeModifier | None = None
    ) -> None:
        """
        Update relationship information about another TaskMixin. Default is no-op.

        Override if necessary.
        """

    def __lshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        """Implements Task << Task."""
        self.set_upstream(other)
        self.set_setup_teardown_ctx_dependencies(other)
        self.set_taskgroup_ctx_dependencies(other)
        return other

    def __rshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        """Implements Task >> Task."""
        self.set_downstream(other)
        self.set_setup_teardown_ctx_dependencies(other)
        self.set_taskgroup_ctx_dependencies(other)
        return other

    def __rrshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        """Called for Task >> [Task] because list don't have __rshift__ operators."""
        self.__lshift__(other)
        return self

    def __rlshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        """Called for Task << [Task] because list don't have __lshift__ operators."""
        self.__rshift__(other)
        return self

    @abstractmethod
    def add_to_taskgroup(self, task_group: TaskGroup) -> None:
        """Add the task to the given task group."""
        raise NotImplementedError()

    @classmethod
    def _iter_references(cls, obj: Any) -> Iterable[tuple[DependencyMixin, str]]:
        from airflow.models.baseoperator import AbstractOperator
        from airflow.utils.mixins import ResolveMixin

        if isinstance(obj, AbstractOperator):
            yield obj, "operator"
        elif isinstance(obj, ResolveMixin):
            yield from obj.iter_references()
        elif isinstance(obj, Sequence):
            for o in obj:
                yield from cls._iter_references(o)

    def set_setup_teardown_ctx_dependencies(self, other: DependencyMixin | Sequence[DependencyMixin]):
        if not SetupTeardownContext.active:
            return
        for op, _ in self._iter_references([self, other]):
            SetupTeardownContext.update_context_map(op)

    def set_taskgroup_ctx_dependencies(self, other: DependencyMixin | Sequence[DependencyMixin]):
        from airflow.utils.task_group import TaskGroupContext

        if not TaskGroupContext.active:
            return
        task_group = TaskGroupContext.get_current_task_group(None)
        for op, _ in self._iter_references([self, other]):
            if task_group:
                op.add_to_taskgroup(task_group)


class TaskMixin(DependencyMixin):
    """Mixin to provide task-related things.

    :meta private:
    """

    def __init_subclass__(cls) -> None:
        warnings.warn(
            f"TaskMixin has been renamed to DependencyMixin, please update {cls.__name__}",
            category=RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return super().__init_subclass__()


class DAGNode(DependencyMixin, metaclass=ABCMeta):
    """
    A base class for a node in the graph of a workflow.

    A node may be an Operator or a Task Group, either mapped or unmapped.
    """

    dag: DAG | None = None
    task_group: TaskGroup | None = None
    """The task_group that contains this node"""

    @property
    @abstractmethod
    def node_id(self) -> str:
        raise NotImplementedError()

    @property
    def label(self) -> str | None:
        tg = self.task_group
        if tg and tg.node_id and tg.prefix_group_id:
            # "task_group_id.task_id" -> "task_id"
            return self.node_id[len(tg.node_id) + 1 :]
        return self.node_id

    start_date: pendulum.DateTime | None
    end_date: pendulum.DateTime | None
    upstream_task_ids: set[str]
    downstream_task_ids: set[str]

    def has_dag(self) -> bool:
        return self.dag is not None

    @property
    def dag_id(self) -> str:
        """Returns dag id if it has one or an adhoc/meaningless ID."""
        if self.dag:
            return self.dag.dag_id
        return "_in_memory_dag_"

    @property
    def log(self) -> Logger:
        raise NotImplementedError()

    @property
    @abstractmethod
    def roots(self) -> Sequence[DAGNode]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def leaves(self) -> Sequence[DAGNode]:
        raise NotImplementedError()

    def _set_relatives(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        upstream: bool = False,
        edge_modifier: EdgeModifier | None = None,
    ) -> None:
        """Sets relatives for the task or task list."""
        from airflow.models.baseoperator import BaseOperator
        from airflow.models.mappedoperator import MappedOperator

        if not isinstance(task_or_task_list, Sequence):
            task_or_task_list = [task_or_task_list]

        task_list: list[Operator] = []
        for task_object in task_or_task_list:
            task_object.update_relative(self, not upstream, edge_modifier=edge_modifier)
            relatives = task_object.leaves if upstream else task_object.roots
            for task in relatives:
                if not isinstance(task, (BaseOperator, MappedOperator)):
                    raise AirflowException(
                        f"Relationships can only be set between Operators; received {task.__class__.__name__}"
                    )
                task_list.append(task)

        # relationships can only be set if the tasks share a single DAG. Tasks
        # without a DAG are assigned to that DAG.
        dags: set[DAG] = {task.dag for task in [*self.roots, *task_list] if task.has_dag() and task.dag}

        if len(dags) > 1:
            raise AirflowException(f"Tried to set relationships between tasks in more than one DAG: {dags}")
        elif len(dags) == 1:
            dag = dags.pop()
        else:
            raise AirflowException(
                f"Tried to create relationships between tasks that don't have DAGs yet. "
                f"Set the DAG for at least one task and try again: {[self, *task_list]}"
            )

        if not self.has_dag():
            # If this task does not yet have a dag, add it to the same dag as the other task.
            self.dag = dag

        for task in task_list:
            if dag and not task.has_dag():
                # If the other task does not yet have a dag, add it to the same dag as this task and
                dag.add_task(task)
            if upstream:
                task.downstream_task_ids.add(self.node_id)
                self.upstream_task_ids.add(task.node_id)
                if edge_modifier:
                    edge_modifier.add_edge_info(self.dag, task.node_id, self.node_id)
            else:
                self.downstream_task_ids.add(task.node_id)
                task.upstream_task_ids.add(self.node_id)
                if edge_modifier:
                    edge_modifier.add_edge_info(self.dag, self.node_id, task.node_id)

    def set_downstream(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ) -> None:
        """Set a node (or nodes) to be directly downstream from the current node."""
        self._set_relatives(task_or_task_list, upstream=False, edge_modifier=edge_modifier)

    def set_upstream(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ) -> None:
        """Set a node (or nodes) to be directly upstream from the current node."""
        self._set_relatives(task_or_task_list, upstream=True, edge_modifier=edge_modifier)

    @property
    def downstream_list(self) -> Iterable[Operator]:
        """List of nodes directly downstream."""
        if not self.dag:
            raise AirflowException(f"Operator {self} has not been assigned to a DAG yet")
        return [self.dag.get_task(tid) for tid in self.downstream_task_ids]

    @property
    def upstream_list(self) -> Iterable[Operator]:
        """List of nodes directly upstream."""
        if not self.dag:
            raise AirflowException(f"Operator {self} has not been assigned to a DAG yet")
        return [self.dag.get_task(tid) for tid in self.upstream_task_ids]

    def get_direct_relative_ids(self, upstream: bool = False) -> set[str]:
        """Get set of the direct relative ids to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_task_ids
        else:
            return self.downstream_task_ids

    def get_direct_relatives(self, upstream: bool = False) -> Iterable[DAGNode]:
        """Get list of the direct relatives to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """This is used by TaskGroupSerialization to serialize a task group's content."""
        raise NotImplementedError()
