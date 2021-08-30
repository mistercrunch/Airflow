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

import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor


class TestPipelineRunStatusSensor(unittest.TestCase):
    def setUp(self):
        self.dag = DAG("test", start_date=datetime(2021, 8, 16), schedule_interval=None, catchup=False)
        self.config = {
            "azure_data_factory_conn_id": "azure_data_factory_test",
            "run_id": "run_id",
            "resource_group_name": "resource-group-name",
            "factory_name": "factory-name",
            "timeout": 100,
            "poke_interval": 15,
        }

    @staticmethod
    def create_pipeline_run(status: str):
        """Helper function to create a mock pipeline run with a given execution status."""

        run = MagicMock()
        run.status = status
        return run

    def test_init(self):
        sensor = AzureDataFactoryPipelineRunStatusSensor(
            task_id="pipeline_run_sensor", dag=self.dag, **self.config
        )
        assert sensor.azure_data_factory_conn_id == self.config["azure_data_factory_conn_id"]
        assert sensor.run_id == self.config["run_id"]
        assert sensor.resource_group_name == self.config["resource_group_name"]
        assert sensor.factory_name == self.config["factory_name"]
        assert sensor.timeout == self.config["timeout"]
        assert sensor.poke_interval == self.config["poke_interval"]

    @parameterized.expand(
        [
            (AzureDataFactoryPipelineRunStatus.SUCCEEDED, True),
            (AzureDataFactoryPipelineRunStatus.FAILED, "AirflowException"),
            (AzureDataFactoryPipelineRunStatus.CANCELLED, "AirflowException"),
            (AzureDataFactoryPipelineRunStatus.CANCELING, False),
            (AzureDataFactoryPipelineRunStatus.QUEUED, False),
            (AzureDataFactoryPipelineRunStatus.IN_PROGRESS, False),
        ]
    )
    def test_poke(self, pipeline_run_status, expected_status):
        mock_pipeline_run = TestPipelineRunStatusSensor.create_pipeline_run(pipeline_run_status)

        with patch.object(
            AzureDataFactoryHook, "get_pipeline_run", return_value=mock_pipeline_run
        ) as mock_get_pipeline_run:
            sensor = AzureDataFactoryPipelineRunStatusSensor(
                task_id="pipeline_run_sensor_poke", dag=self.dag, **self.config
            )

            if expected_status == "AirflowException":
                # The sensor should fail if the pipeline run fails or is canceled.
                with pytest.raises(
                    AirflowException,
                    match=f"Pipeline run {self.config['run_id']} is in a negative terminal status: "
                    f"{pipeline_run_status}",
                ):
                    sensor.poke({})
            else:
                assert sensor.poke({}) == expected_status

            mock_get_pipeline_run.assert_called_once_with(
                run_id=self.config["run_id"],
                factory_name=self.config["factory_name"],
                resource_group_name=self.config["resource_group_name"],
            )
