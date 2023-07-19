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

from unittest import mock
from unittest.mock import MagicMock

import pytest as pytest

import airflow
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.operators.datapipeline import (
    CreateDataPipelineOperator,
    RunDataPipelineOperator,
)
from airflow.providers.google.cloud.hooks.datapipeline import DataPipelineHook
from airflow.version import version

TASK_ID = "test-datapipeline-operators"
TEST_BODY = {
    "name": "projects/test-datapipeline-operators/locations/test-location/pipelines/test-pipeline",
            "type": "PIPELINE_TYPE_BATCH",
            "workload": {
                "dataflowFlexTemplateRequest": {
                "launchParameter": {
                    "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/Word_Count_metadata",
                    "jobName": "test-job",
                    "environment": {
                    "tempLocation": "test-temp-location"
                    },
                    "parameters": {
                    "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
                    "output": "gs://test/output/my_output"
                    }
                },
                "projectId": "test-project-id",
                "location": "test-location"
                }
            }
}
TEST_LOCATION = "test-location"
TEST_PROJECTID = "test-project-id"
TEST_GCP_CONN_ID = "test_gcp_conn_id"

class TestCreateDataPipelineOperator:
    @pytest.fixture
    def create_operator(self):
        """ 
        Creates a mock create datapipeline operator to be used in testing.
        """
        return CreateDataPipelineOperator(
            task_id = "test_create_datapipeline",
            body = TEST_BODY,
            project_id = TEST_PROJECTID,
            location = TEST_LOCATION,
            gcp_conn_id = TEST_GCP_CONN_ID
        )
    
    @mock.patch("airflow.providers.google.cloud.operators.datapipeline.DataPipelineHook")
    def test_execute(self, mock_datapipeline, create_operator):
        """ 
        Test if the execute function creates and calls the DataPipeline hook with the correct parameters
        """
        create_operator.execute(mock.MagicMock())
        mock_datapipeline.assert_called_once_with(
            gcp_conn_id = "test_gcp_conn_id",
        )
        
        mock_datapipeline.return_value.create_data_pipeline.assert_called_once_with(
            project_id = TEST_PROJECTID,
            body = TEST_BODY,
            location = TEST_LOCATION
        )
