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

import logging
from functools import wraps
from typing import TYPE_CHECKING, cast

import pendulum
from pendulum import DateTime
from sqlalchemy import func, select

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import Conflict, NotFound
from airflow.api_connexion.schemas.backfill_schema import (
    BackfillCollection,
    backfill_collection_schema,
    backfill_schema,
)
from airflow.exceptions import AirflowException
from airflow.models.backfill import Backfill, BackfillDagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType
from airflow.www.decorators import action_logging

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse

log = logging.getLogger(__name__)

RESOURCE_EVENT_PREFIX = "dag"


def backfill_to_dag(func):
    """
    Enrich the request with dag_id.

    :meta private:
    """

    @wraps(func)
    def wrapper(*, backfill_id, session, **kwargs):
        backfill = session.get(Backfill, backfill_id)
        if not backfill:
            raise NotFound("Backfill not found")
        return func(dag_id=backfill.dag_id, backfill_id=backfill_id, session=session, **kwargs)

    return wrapper


class AlreadyRunningBackfill(AirflowException):
    """Raised when attempting to create backfill and one already active."""


@provide_session
def _create_backfill(
    *,
    dag_id: str,
    from_date: DateTime,
    to_date: DateTime,
    max_active_runs: int,
    reverse: bool,
    dag_run_conf: dict | None,
    session: Session = NEW_SESSION,
) -> Backfill | None:
    serdag = session.get(SerializedDagModel, dag_id)
    if not serdag:
        raise NotFound(f"Could not find dag {dag_id}")

    num_active = session.scalar(
        select(func.count()).where(Backfill.dag_id == dag_id, Backfill.completed_at.is_(None))
    )
    if num_active > 0:
        raise AlreadyRunningBackfill(
            f"Another backfill is running for dag {dag_id}. "
            f"There can be only one running backfill per dag."
        )

    br = Backfill(
        dag_id=dag_id,
        from_date=from_date,
        to_date=to_date,
        max_active_runs=max_active_runs,
        dag_run_conf=dag_run_conf,
    )
    session.add(br)
    session.commit()

    dag = serdag.dag
    depends_on_past = any(x.depends_on_past for x in dag.tasks)
    if depends_on_past:
        if reverse is True:
            raise ValueError(
                "Backfill cannot be run in reverse when the dag has tasks where depends_on_past=True"
            )

    backfill_sort_ordinal = 0
    dagrun_info_list = dag.iter_dagrun_infos_between(from_date, to_date)
    if reverse:
        dagrun_info_list = reversed([x for x in dag.iter_dagrun_infos_between(from_date, to_date)])
    for info in dagrun_info_list:
        backfill_sort_ordinal += 1
        log.info("creating backfill dag run %s dag_id=%s backfill_id=%s, info=", dag.dag_id, br.id, info)
        dr = None
        try:
            dr = dag.create_dagrun(
                triggered_by=DagRunTriggeredByType.BACKFILL,
                execution_date=info.logical_date,
                data_interval=info.data_interval,
                start_date=timezone.utcnow(),
                state=DagRunState.QUEUED,
                external_trigger=False,
                conf=br.dag_run_conf,
                run_type=DagRunType.BACKFILL_JOB,
                creating_job_id=None,
                session=session,
            )
        except Exception:
            dag.log.exception("something failed")
            session.rollback()
        session.add(
            BackfillDagRun(
                backfill_id=br.id,
                dag_run_id=dr.id if dr else None,  # this means we failed to create the dag run
                sort_ordinal=backfill_sort_ordinal,
            )
        )
        session.commit()
    return br


@security.requires_access_dag("GET")
@action_logging
@provide_session
def list_backfills(dag_id, session):
    backfills = session.scalars(select(Backfill).where(Backfill.dag_id == dag_id)).all()
    obj = BackfillCollection(
        backfills=backfills,
        total_entries=len(backfills),
    )
    return backfill_collection_schema.dump(obj)


@provide_session
@backfill_to_dag
@security.requires_access_dag("PUT")
@action_logging
def pause_backfill(*, backfill_id, session, **kwargs):
    br = session.get(Backfill, backfill_id)
    if br.completed_at:
        raise Conflict("Backfill is already completed.")
    if br.is_paused is False:
        br.is_paused = True
    session.commit()
    return backfill_schema.dump(br)


@provide_session
@backfill_to_dag
@security.requires_access_dag("PUT")
@action_logging
def unpause_backfill(*, backfill_id, session, **kwargs):
    br = session.get(Backfill, backfill_id)
    if br.completed_at:
        raise Conflict("Backfill is already completed.")
    if br.is_paused:
        br.is_paused = False
    session.commit()
    return backfill_schema.dump(br)


@provide_session
@backfill_to_dag
@security.requires_access_dag("PUT")
@action_logging
def cancel_backfill(*, backfill_id, session, **kwargs):
    br: Backfill = session.get(Backfill, backfill_id)
    if br.completed_at is not None:
        raise Conflict("Backfill is already completed.")

    br.completed_at = timezone.utcnow()

    # first, pause
    if not br.is_paused:
        br.is_paused = True
    session.commit()
    return backfill_schema.dump(br)


@provide_session
@backfill_to_dag
@security.requires_access_dag("GET")
@action_logging
def get_backfill(*, backfill_id: int, session: Session = NEW_SESSION, **kwargs):
    backfill = session.get(Backfill, backfill_id)
    if backfill:
        return backfill_schema.dump(backfill)
    raise NotFound("Backfill not found")


@security.requires_access_dag("PUT")
@action_logging
def create_backfill(
    dag_id: str,
    from_date: str,
    to_date: str,
    max_active_runs: int = 10,
    reverse: bool = False,
    dag_run_conf: dict | None = None,
) -> APIResponse:
    try:
        backfill_obj = _create_backfill(
            dag_id=dag_id,
            from_date=cast(DateTime, pendulum.parse(from_date)),
            to_date=cast(DateTime, pendulum.parse(to_date)),
            max_active_runs=max_active_runs,
            reverse=reverse,
            dag_run_conf=dag_run_conf,
        )
        return backfill_schema.dump(backfill_obj)
    except AlreadyRunningBackfill:
        raise Conflict(f"There is already a running backfill for dag {dag_id}")
