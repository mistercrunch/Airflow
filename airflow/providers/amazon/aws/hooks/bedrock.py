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

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class BedrockHook(AwsBaseHook):
    """
    Interact with Amazon Bedrock.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock") <Bedrock.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)

    def _get_job_by_name(self, job_name: str):
        return self.conn.get_model_customization_job(jobIdentifier=job_name)

    def get_customize_model_job_state(self, job_name) -> str:
        state = self._get_job_by_name(job_name)["status"]
        self.log.info("Job '%s' state: %s", job_name, state)
        return state

    def job_name_exists(self, job_name: str) -> bool:
        try:
            self._get_job_by_name(job_name)
            self.log.info("Verified that job name '%s' does exist.", job_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                self.log.info("Job name '%s' does not exist.", job_name)
                return False
            raise e

    def get_job_arn(self, job_name: str) -> str:
        return self._get_job_by_name(job_name)["jobArn"]


class BedrockRuntimeHook(AwsBaseHook):
    """
    Interact with the Amazon Bedrock Runtime.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-runtime") <BedrockRuntime.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-runtime"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)
