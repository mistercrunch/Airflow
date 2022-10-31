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

import unittest
from unittest import mock

from airflow.providers.amazon.aws.operators.s3 import S3ListPrefixesOperator

TASK_ID = "test-s3-list-prefixes-operator"
BUCKET = "test-bucket"
DELIMITER = "/"
PREFIX = "test/"
MOCK_SUBFOLDERS = ["test/"]


class TestS3ListOperator(unittest.TestCase):
    @mock.patch("airflow.providers.amazon.aws.operators.s3.S3Hook")
    def test_execute(self, mock_hook):

        mock_hook.return_value.list_prefixes.return_value = MOCK_SUBFOLDERS

        operator = S3ListPrefixesOperator(task_id=TASK_ID, bucket=BUCKET, prefix=PREFIX, delimiter=DELIMITER)

        subfolders = operator.execute(None)

        mock_hook.return_value.list_prefixes.assert_called_once_with(
            bucket_name=BUCKET, prefix=PREFIX, delimiter=DELIMITER
        )
        assert subfolders == MOCK_SUBFOLDERS
