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
"""Hook for Cloudant."""

from __future__ import annotations

from typing import Any

from ibmcloudant import CloudantV1, CouchDbSessionAuthenticator

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection


class CloudantHook(BaseHook):
    """
    Interact with Cloudant. This class is a thin wrapper around the cloudant python library.

    .. seealso:: the latest documentation `here <https://python-cloudant.readthedocs.io/en/latest/>`_.

    :param cloudant_conn_id: The connection id to authenticate and get a session object from cloudant.
    """

    conn_name_attr = "cloudant_conn_id"
    default_conn_name = "cloudant_default"
    conn_type = "cloudant"
    hook_name = "Cloudant"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["port", "extra"],
            "relabeling": {"host": "Account", "login": "Username (or API Key)", "schema": "Database"},
        }

    def __init__(self, cloudant_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.cloudant_conn_id = cloudant_conn_id

    def get_conn(self) -> CloudantV1:
        """
        Open a connection to the cloudant service and close it automatically if used as context manager.

        .. note::
            In the connection form:
            - 'host' equals the 'Account' (required)
            - 'login' equals the 'Username (or API Key)' (required)
            - 'password' equals the 'Password' (required)

        :return: a CloudantV1 service object backed by a session-based user/password authenticator.
        """
        conn = self.get_connection(self.cloudant_conn_id)

        self._validate_connection(conn)

        authenticator = CouchDbSessionAuthenticator(username=conn.login, password=conn.password)
        service = CloudantV1(authenticator=authenticator)
        service.set_service_url(f"https://{conn.host}.cloudant.com")

        return service

    @staticmethod
    def _validate_connection(conn: Connection) -> None:
        for conn_param in ["host", "login", "password"]:
            if not getattr(conn, conn_param):
                raise AirflowException(f"missing connection parameter {conn_param}")
