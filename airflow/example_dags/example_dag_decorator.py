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

from typing import TYPE_CHECKING, Any

import httpx
import pendulum

from airflow.decorators import dag, task
from airflow.models.baseoperator import BaseOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class GetRequestOperator(BaseOperator):
    """Custom operator to send GET request to provided url"""

    def __init__(self, *, url: str, **kwargs):
        super().__init__(**kwargs)
        self.url = url

    def execute(self, context: Context):
        return httpx.get(self.url).json()


# [START dag_decorator_usage]
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def example_dag_decorator(to_email: str = "example1@example.com", from_email: str = "example2@example.com"):
    """
    DAG to send server IP to email.

    :param to_email: Email to send IP to. Defaults to exampl1e@example.com.
    :param from_email: Email to send IP from. Defaults to example2@example.com
    """
    get_ip = GetRequestOperator(task_id="get_ip", url="http://httpbin.org/get")

    @task(multiple_outputs=True)
    def prepare_email(raw_json: dict[str, Any]) -> dict[str, str]:
        external_ip = raw_json["origin"]
        return {
            "subject": f"Server connected from {external_ip}",
            "body": f"Seems like today your server executing Airflow is connected from IP {external_ip}<br>",
        }

    email_info = prepare_email(get_ip.output)

    EmailOperator(
        task_id="send_email",
        to=to_email,
        from_email=from_email,
        subject=email_info["subject"],
        html_content=email_info["body"],
    )


example_dag = example_dag_decorator()
# [END dag_decorator_usage]
