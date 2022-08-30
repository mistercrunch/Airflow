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
from copy import deepcopy
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from dateutil.tz import tzlocal

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.emr import EmrStepsSensor

LIST_JOB_STEP_BASIC_RETURN = {
    "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "8dee8db2-3719-11e6-9e20-35b2f861a2a6"},
    "Steps": [],
}

LIST_JOB_STEP_RUNNING_RETURN = {
    "ActionOnFailure": "CONTINUE",
    "Config": {
        "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        "Jar": "command-runner.jar",
        "Properties": {},
    },
    "Id": "s-VK57YR1Z9Z5N",
    "Name": "calculate_pi",
    "Status": {
        "State": "RUNNING",
        "StateChangeReason": {},
        "Timeline": {
            "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
            "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
        },
    },
}

DESCRIBE_JOB_STEP_CANCELLED_RETURN = {
    "ActionOnFailure": "CONTINUE",
    "Config": {
        "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        "Jar": "command-runner.jar",
        "Properties": {},
    },
    "Id": "s-VK57YR1Z9Z5N",
    "Name": "calculate_pi",
    "Status": {
        "State": "CANCELLED",
        "StateChangeReason": {},
        "Timeline": {
            "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
            "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
        },
    },
}

LIST_JOB_STEP_FAILED_RETURN = {
    "ActionOnFailure": "CONTINUE",
    "Config": {
        "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        "Jar": "command-runner.jar",
        "Properties": {},
    },
    "Id": "s-VK57YR1Z9Z5N",
    "Name": "calculate_pi",
    "Status": {
        "State": "FAILED",
        "StateChangeReason": {},
        "FailureDetails": {
            "LogFile": "s3://fake-log-files/emr-logs/j-8989898989/steps/s-VK57YR1Z9Z5N",
            "Reason": "Unknown Error.",
        },
        "Timeline": {
            "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
            "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
        },
    },
}

LIST_JOB_STEP_INTERRUPTED_RETURN = {
    "ActionOnFailure": "CONTINUE",
    "Config": {
        "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        "Jar": "command-runner.jar",
        "Properties": {},
    },
    "Id": "s-VK57YR1Z9Z5N",
    "Name": "calculate_pi",
    "Status": {
        "State": "INTERRUPTED",
        "StateChangeReason": {},
        "Timeline": {
            "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
            "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
        },
    },
}

LIST_JOB_STEP_COMPLETED_RETURN = {
    "ActionOnFailure": "CONTINUE",
    "Config": {
        "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        "Jar": "command-runner.jar",
        "Properties": {},
    },
    "Id": "s-VK57YR1Z9Z5N",
    "Name": "calculate_pi",
    "Status": {
        "State": "COMPLETED",
        "StateChangeReason": {},
        "Timeline": {
            "CreationDateTime": datetime(2016, 6, 20, 19, 0, 18, tzinfo=tzlocal()),
            "StartDateTime": datetime(2016, 6, 20, 19, 2, 34, tzinfo=tzlocal()),
        },
    },
}


class TestEmrStepsSensor:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.emr_client_mock = MagicMock()
        self.sensor = EmrStepsSensor(
            task_id="test_task",
            poke_interval=0,
            job_flow_id="j-8989898989",
            aws_conn_id="aws_default",
        )

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.emr_client_mock

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)
        self.return_value_running = deepcopy(LIST_JOB_STEP_BASIC_RETURN)
        self.return_value_running["Steps"].append(LIST_JOB_STEP_RUNNING_RETURN)
        self.return_value_running["Steps"].append(LIST_JOB_STEP_RUNNING_RETURN)

    def test_steps_all_completed(self):
        return_value_complete = deepcopy(LIST_JOB_STEP_BASIC_RETURN)
        return_value_complete["Steps"].append(LIST_JOB_STEP_COMPLETED_RETURN)
        return_value_complete["Steps"].append(LIST_JOB_STEP_COMPLETED_RETURN)

        self.emr_client_mock.list_steps.side_effect = [self.return_value_running, return_value_complete]

        with patch("boto3.session.Session", self.boto3_session_mock):
            self.sensor.execute(None)

            assert self.emr_client_mock.list_steps.call_count == 2
            calls = [
                unittest.mock.call(ClusterId="j-8989898989"),
                unittest.mock.call(ClusterId="j-8989898989"),
            ]
            self.emr_client_mock.list_steps.assert_has_calls(calls)

    def test_steps_one_complete_and_one_failed(self):
        return_value_one_failed = deepcopy(LIST_JOB_STEP_BASIC_RETURN)
        return_value_one_failed["Steps"].append(LIST_JOB_STEP_COMPLETED_RETURN)
        return_value_one_failed["Steps"].append(LIST_JOB_STEP_FAILED_RETURN)

        self.emr_client_mock.list_steps.side_effect = [self.return_value_running, return_value_one_failed]

        with patch("boto3.session.Session", self.boto3_session_mock):
            with pytest.raises(AirflowException):
                self.sensor.execute(None)

    def test_steps_all_failed(self):
        return_value = deepcopy(LIST_JOB_STEP_BASIC_RETURN)
        return_value["Steps"].append(LIST_JOB_STEP_FAILED_RETURN)
        return_value["Steps"].append(LIST_JOB_STEP_FAILED_RETURN)

        self.emr_client_mock.list_steps.side_effect = [self.return_value_running, return_value]

        with patch("boto3.session.Session", self.boto3_session_mock):
            with pytest.raises(AirflowException):
                self.sensor.execute(None)
