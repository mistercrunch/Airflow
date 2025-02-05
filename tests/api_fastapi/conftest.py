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

import os

import pytest
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app
from airflow.models.dag_version import DagVersion
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.db import parse_and_sync_to_db


@pytest.fixture
def test_client():
    return TestClient(create_app())


@pytest.fixture
def client():
    """This fixture is more flexible than test_client, as it allows to specify which apps to include."""

    def create_test_client(apps="all"):
        app = create_app(apps=apps)
        return TestClient(app)

    return create_test_client


@pytest.fixture
def make_dag_with_multiple_versions(dag_maker):
    """
    Create DAG with multiple versions

    Version 1 will have 1 task, version 2 will have 2 tasks, and version 3 will have 3 tasks.
    """
    dag_id = "dag_with_multiple_versions"

    for version_number in range(1, 4):
        with dag_maker(dag_id) as dag:
            for i in range(version_number):
                EmptyOperator(task_id=f"task{i+1}")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag, bundle_name="dag_maker")
        dag_maker.create_dagrun(
            run_id=f"run{i+1}",
            dag_version=DagVersion.get_version(dag_id=dag_id, version_number=version_number),
        )


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    parse_and_sync_to_db(os.devnull, include_examples=True)
    return DagBag(read_dags_from_db=True)
