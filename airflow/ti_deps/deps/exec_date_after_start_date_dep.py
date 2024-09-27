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

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType


class ExecDateAfterStartDateDep(BaseTIDep):
    """Determines whether a task's execution date is after start date."""

    NAME = "Execution Date"
    IGNORABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        # todo: AIP-78 can we pass DR in the dep context instead, to avoid a query?
        dagrun = ti.get_dagrun(session)
        if dagrun.run_type == DagRunType.BACKFILL_JOB:
            return
        # todo: should we log actual reasons for not running rather than having webserver re-evaluate?
        if ti.task.start_date and ti.execution_date < ti.task.start_date:
            yield self._failing_status(
                reason=(
                    f"The execution date is {ti.execution_date.isoformat()} but this is before "
                    f"the task's start date {ti.task.start_date.isoformat()}."
                )
            )

        if ti.task.dag and ti.task.dag.start_date and ti.execution_date < ti.task.dag.start_date:
            yield self._failing_status(
                reason=(
                    f"The execution date is {ti.execution_date.isoformat()} but this is "
                    f"before the task's DAG's start date {ti.task.dag.start_date.isoformat()}."
                )
            )
