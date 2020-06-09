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

from flask import request

from airflow.api_connexion import parameters
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.schemas.dag_run_schema import (
    DAGRunCollection, dagrun_collection_schema, dagrun_schema,
)
from airflow.api_connexion.utils import parse_datetime_in_query
from airflow.models import DagRun
from airflow.utils.session import provide_session


def delete_dag_run():
    """
    Delete a DAG Run
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def get_dag_run(dag_id, dag_run_id, session):
    """
    Get a DAG Run.
    """
    query = session.query(DagRun)
    query = query.filter(DagRun.dag_id == dag_id)
    query = query.filter(DagRun.run_id == dag_run_id)
    dag_run = query.one_or_none()
    if dag_run is None:
        raise NotFound("DAGRun not found")
    return dagrun_schema.dump(dag_run)


@provide_session
def get_dag_runs(dag_id, session):
    """
    Get all DAG Runs.
    """
    offset = request.args.get(parameters.page_offset, 0)
    limit = min(int(request.args.get(parameters.page_limit, 100)), 100)

    start_date_gte = parse_datetime_in_query(
        request.args.get(parameters.filter_start_date_gte, None)
    )
    start_date_lte = parse_datetime_in_query(
        request.args.get(parameters.filter_start_date_lte, None)
    )

    execution_date_gte = parse_datetime_in_query(
        request.args.get(parameters.filter_execution_date_gte, None)
    )
    execution_date_lte = parse_datetime_in_query(
        request.args.get(parameters.filter_execution_date_lte, None)
    )

    end_date_gte = parse_datetime_in_query(
        request.args.get(parameters.filter_end_date_gte, None)
    )
    end_date_lte = parse_datetime_in_query(
        request.args.get(parameters.filter_end_date_lte, None)
    )

    query = session.query(DagRun)

    #  This endpoint allows specifying ~ as the dag_id to retrieve DAG Runs for all DAGs.
    if dag_id != '~':
        query = query.filter(DagRun.dag_id == dag_id)

    # filter start date

    if start_date_gte:
        query = query.filter(DagRun.start_date >= start_date_gte)

    if start_date_lte:
        query = query.filter(DagRun.start_date <= start_date_lte)

    # filter execution date
    if execution_date_gte:
        query = query.filter(DagRun.execution_date >= execution_date_gte)

    if execution_date_lte:
        query = query.filter(DagRun.execution_date <= execution_date_lte)

    # filter end date
    if end_date_gte and not end_date_lte:
        query = query.filter(DagRun.end_date >= end_date_gte)

    if end_date_lte and not end_date_gte:
        query = query.filter(DagRun.end_date <= end_date_lte)

    # apply offset and limit
    dag_run = query.offset(offset).limit(limit)
    total_entries = session.query(DagRun).all()

    return dagrun_collection_schema.dump(DAGRunCollection(dag_runs=dag_run,
                                                          total_entries=len(total_entries)))


def get_dag_runs_batch():
    """
    Get list of DAG Runs
    """
    raise NotImplementedError("Not implemented yet.")


def patch_dag_run():
    """
    Update a DAG Run
    """
    raise NotImplementedError("Not implemented yet.")


def post_dag_run():
    """
    Trigger a DAG.
    """
    raise NotImplementedError("Not implemented yet.")
