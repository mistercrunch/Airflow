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

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from airflow.models import BaseOperator, BaseOperatorLink, TaskInstance
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunException, DbtCloudJobRunStatus

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DbtCloudRunJobOperatorLink(BaseOperatorLink):
    name = "Monitor Job Run"

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        job_run_url = ti.xcom_pull(task_ids=operator.task_id, key="job_run_url")

        return job_run_url


class DbtCloudRunJobOperator(BaseOperator):
    """
    Executes a dbt Cloud job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DbtCloudRunJobOperator`

    :param dbt_cloud_conn_id: The connection ID for connecting to dbt Cloud.
    :type dbt_cloud_conn_id: str
    :param job_id: The ID of a dbt Cloud job.
    :type job_id: int
    :param account_id: Optional. The ID of a dbt Cloud account.
    :type account_id: int
    :param trigger_reason: Optional. Description of the reason to trigger the job.
    :type trigger_reason: str
    :param steps_override: Optional. List of dbt commands to execute when triggering the job instead of those
        configured in dbt Cloud.
    :type steps_override: List[str]
    :param schema_override: Optional. Override the destination schema in the configured target for this job.
    :type schema_override: str
    :param wait_for_termination: Flag to wait on a job run's termination.  By default, this feature is
        enabled but could be disabled to perform an asynchronous wait for a long-running job run execution
        using the ``DbtCloudJobRunSensor``.
    :type wait_for_termination: bool
    :param timeout: Time in seconds to wait for a job run to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True. Defaults to 7 days.
    :type timeout: int
    :param check_interval: Time in seconds to check on a job run's status for non-asynchronous waits.
        Used only if ``wait_for_termination`` is True. Defaults to 60 seconds.
    :type check_interval: int
    :param additional_run_config: Optional. Any additional parameters that should be included in the API
        request when triggering the job.
    :type additional_run_config: Dict[str, Any]
    """

    template_fields = ("dbt_cloud_conn_id", "job_id", "account_id", "trigger_reason")

    operator_extra_links = (DbtCloudRunJobOperatorLink(),)

    def __init__(
        self,
        *,
        dbt_cloud_conn_id: str = DbtCloudHook.default_conn_name,
        job_id: int,
        account_id: Optional[int] = None,
        trigger_reason: Optional[str] = None,
        steps_override: Optional[List[str]] = None,
        schema_override: Optional[str] = None,
        wait_for_termination: bool = False,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        additional_run_config: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.hook = DbtCloudHook(self.dbt_cloud_conn_id)
        self.account_id = account_id
        self.job_id = job_id
        self.trigger_reason = trigger_reason
        self.steps_override = steps_override
        self.schema_override = schema_override
        self.wait_for_termination = wait_for_termination
        self.timeout = timeout
        self.check_interval = check_interval
        self.additional_run_config = additional_run_config or {}
        self.run_id: int

    def execute(self, context: "Context") -> int:
        if self.trigger_reason is None:
            self.trigger_reason = (
                f"Triggered in Airflow by task {self.task_id!r} in the {self.dag.dag_id} DAG."
            )
        print(self.additional_run_config)
        trigger_job_response = self.hook.trigger_job_run(
            account_id=self.account_id,
            job_id=self.job_id,
            cause=self.trigger_reason,
            additional_run_config=self.additional_run_config,
        )
        self.run_id = trigger_job_response.json()["data"]["id"]
        job_run_url = trigger_job_response.json()["data"]["href"]
        # Push the ``job_run_url`` value to XCom regardless of what happens during execution so that the job
        # run can be monitored via the operator link.
        context["ti"].xcom_push(key="job_run_url", value=job_run_url)

        if self.wait_for_termination:
            self.log.info(f"Waiting for job run {self.run_id} to terminate.")

            if self.hook.wait_for_job_run_status(
                run_id=self.run_id,
                account_id=self.account_id,
                expected_statuses=DbtCloudJobRunStatus.SUCCESS.value,
                check_interval=self.check_interval,
                timeout=self.timeout,
            ):
                self.log.info(f"Job run {self.run_id} has completed successfully.")
            else:
                raise DbtCloudJobRunException(f"Job run {self.run_id} has failed or has been cancelled.")

        return self.run_id

    def on_kill(self) -> None:
        if self.run_id:
            self.hook.cancel_job_run(account_id=self.account_id, run_id=self.run_id)
            self.log.info(f"Job run {self.run_id} has been cancelled successfully.")


class DbtCloudGetJobRunArtifactOperator(BaseOperator):
    """
    Download artifacts from a dbt Cloud job run.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DbtCloudGetJobRunArtifactOperator`

    :param dbt_cloud_conn_id: The connection ID for connecting to dbt Cloud.
    :type dbt_cloud_conn_id: str
    :param run_id: The ID of a dbt Cloud job run.
    :type run_id: int
    :param path: The file path related to the artifact file. Paths are rooted at the target/ directory.
        Use "manifest.json", "catalog.json", or "run_results.json" to download dbt-generated artifacts
        for the run.
    :type path: str
    :param account_id: Optional. The ID of a dbt Cloud account.
    :type account_id: int
    :param step: Optional. The index of the Step in the Run to query for artifacts. The first step in the
        run has the index 1. If the step parameter is omitted, artifacts for the last step in the run will
        be returned.
    :type step: int
    :param output_file_name: Optional. The desired file name for the download artifact file.
        Defaults to <run_id>_<path> (e.g. "728368_run_results.json").
    """

    template_fields = ("dbt_cloud_conn_id", "run_id", "path", "account_id", "output_file_name")

    def __init__(
        self,
        *,
        dbt_cloud_conn_id: str = DbtCloudHook.default_conn_name,
        run_id: int,
        path: str,
        account_id: Optional[int] = None,
        step: Optional[int] = None,
        output_file_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.hook = DbtCloudHook(self.dbt_cloud_conn_id)
        self.run_id = run_id
        self.path = path
        self.account_id = account_id
        self.step = step
        self.output_file_name = output_file_name or f"{self.run_id}_{self.path}"

    def execute(self, context: "Context") -> None:
        artifact_data = self.hook.get_job_run_artifact(
            run_id=self.run_id, path=self.path, account_id=self.account_id, step=self.step
        )

        with open(self.output_file_name, "w") as file:
            json.dump(artifact_data.json(), file)
