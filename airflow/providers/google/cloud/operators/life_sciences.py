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
"""Operators that interact with Google Cloud Life Sciences service."""

from typing import Iterable, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.life_sciences import LifeSciencesHook
from airflow.utils.decorators import apply_defaults


class LifeSciencesRunPipelineOperator(BaseOperator):
    """
    Runs a pipeline

    :param body: The request body
    :param location: The location of the project
    :param project_id: ID of the Google Cloud project if None then
        default project_id is used.
    The connection ID to use to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (for example v2beta).
    :type api_version: str
    """

    template_fields = ("body", "gcp_conn_id", "api_version")  # type: Iterable[str]

    @apply_defaults
    def __init__(self,
                 body: dict,
                 location: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = "google_cloud_default",
                 api_version: str = "v2beta",
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.body = body
        self.location = location
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.body:
            raise AirflowException("The required parameter 'body' is missing")
        if not self.location:
            raise AirflowException("The required parameter 'location' is missing")

    def execute(self, context):
        hook = LifeSciencesHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)

        return hook.run_pipeline(body=self.body,
                                 location=self.location,
                                 project_id=self.project_id)
