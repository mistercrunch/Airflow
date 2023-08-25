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

from dataclasses import dataclass

from airflow.auth.managers.models.base_user import BaseUser
from airflow.auth.managers.models.resource_details import ResourceDetails
from airflow.auth.managers.models.resource_method import ResourceMethod
from airflow.auth.managers.models.resource_type import ResourceType


@dataclass
class AuthorizedAction:
    """Represents an action that is being checked for authorization."""

    action: ResourceMethod
    resource_type: ResourceType
    resource_details: ResourceDetails | None = None
    user: BaseUser | None = None
