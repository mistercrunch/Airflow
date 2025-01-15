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

from typing import TYPE_CHECKING

from airflow.dag_processing.dag_parser import DagParser
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job, perform_heartbeat
from airflow.stats import Stats
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class DagProcessorJobRunner(BaseJobRunner, LoggingMixin):
    """
    DagProcessorJobRunner is a job runner that runs a DagParser.

    :param job: Job instance to use
    """

    job_type = "DagProcessorJob"

    def __init__(
        self,
        job: Job,
        *args,
        **kwargs,
    ):
        super().__init__(job)
        self._heartbeat = lambda: perform_heartbeat(
            job=self.job,
            heartbeat_callback=self.heartbeat_callback,
            only_if_necessary=True,
        )

    def _execute(self) -> int | None:
        self.log.info("Starting the Dag Processor Job")
        try:
            DagParser().run_ingestion(heartbeat_callback=self._heartbeat)
        except Exception:
            self.log.exception("Exception when executing DagProcessorJob")
            raise
        return None

    @provide_session
    def heartbeat_callback(self, session: Session = NEW_SESSION) -> None:
        Stats.incr("dag_processor_heartbeat", 1, 1)
