# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
from airflow import configuration
from airflow.contrib.hooks.aws_glue_job_hook import AwsGlueJobHook
from airflow.contrib.sensors.aws_glue_job_sensor import AwsGlueJobSensor


try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestAwsGlueJobSensor(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()

    @mock.patch.object(AwsGlueJobHook, 'get_conn')
    @mock.patch.object(AwsGlueJobHook, 'job_completion')
    def test_poke(self, mock_job_completion, mock_conn):
        mock_conn.return_value.get_job_run()
        mock_job_completion.return_value = 'SUCCEEDED'
        op = AwsGlueJobSensor(task_id='test_glue_job_sensor',
                              job_name='aws_test_glue_job',
                              run_id='5152fgsfsjhsh61661',
                              poke_interval=1,
                              timeout=5,
                              aws_conn_id='aws_default')
        self.assertTrue(op.poke(None))

    @mock.patch.object(AwsGlueJobHook, 'get_conn')
    @mock.patch.object(AwsGlueJobHook, 'job_completion')
    def test_poke_false(self, mock_job_completion, mock_conn):
        mock_conn.return_value.get_job_run()
        mock_job_completion.return_value = 'RUNNING'
        op = AwsGlueJobSensor(task_id='test_glue_job_sensor',
                              job_name='aws_test_glue_job',
                              run_id='5152fgsfsjhsh61661',
                              poke_interval=1,
                              timeout=5,
                              aws_conn_id='aws_default')
        self.assertFalse(op.poke(None))


if __name__ == '__main__':
    unittest.main()
