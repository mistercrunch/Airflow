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

# generated by datamodel-codegen:
#   filename:  http://0.0.0.0:9091/execution/openapi.json
#   version:   0.26.3

from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class AssetProfile(BaseModel):
    """
    Profile of an Asset.

    Asset will have name, uri and asset_type defined.
    AssetNameRef will have name and asset_type defined.
    AssetUriRef will have uri and asset_type defined.
    """

    name: Annotated[str | None, Field(title="Name")] = None
    uri: Annotated[str | None, Field(title="Uri")] = None
    asset_type: Annotated[str, Field(title="Asset Type")]


class AssetResponse(BaseModel):
    """
    Asset schema for responses with fields that are needed for Runtime.
    """

    name: Annotated[str, Field(title="Name")]
    uri: Annotated[str, Field(title="Uri")]
    group: Annotated[str, Field(title="Group")]
    extra: Annotated[dict[str, Any] | None, Field(title="Extra")] = None


class ConnectionResponse(BaseModel):
    """
    Connection schema for responses with fields that are needed for Runtime.
    """

    conn_id: Annotated[str, Field(title="Conn Id")]
    conn_type: Annotated[str, Field(title="Conn Type")]
    host: Annotated[str | None, Field(title="Host")] = None
    schema_: Annotated[str | None, Field(alias="schema", title="Schema")] = None
    login: Annotated[str | None, Field(title="Login")] = None
    password: Annotated[str | None, Field(title="Password")] = None
    port: Annotated[int | None, Field(title="Port")] = None
    extra: Annotated[str | None, Field(title="Extra")] = None


class DagRunType(str, Enum):
    """
    Class with DagRun types.
    """

    BACKFILL = "backfill"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    ASSET_TRIGGERED = "asset_triggered"


class IntermediateTIState(str, Enum):
    """
    States that a Task Instance can be in that indicate it is not yet in a terminal or running state.
    """

    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RESTARTING = "restarting"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    DEFERRED = "deferred"


class PrevSuccessfulDagRunResponse(BaseModel):
    """
    Schema for response with previous successful DagRun information for Task Template Context.
    """

    data_interval_start: Annotated[datetime | None, Field(title="Data Interval Start")] = None
    data_interval_end: Annotated[datetime | None, Field(title="Data Interval End")] = None
    start_date: Annotated[datetime | None, Field(title="Start Date")] = None
    end_date: Annotated[datetime | None, Field(title="End Date")] = None


class TIDeferredStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to a deferred state.
    """

    state: Annotated[Literal["deferred"] | None, Field(title="State")] = "deferred"
    classpath: Annotated[str, Field(title="Classpath")]
    trigger_kwargs: Annotated[dict[str, Any] | None, Field(title="Trigger Kwargs")] = None
    next_method: Annotated[str, Field(title="Next Method")]
    trigger_timeout: Annotated[timedelta | None, Field(title="Trigger Timeout")] = None


class TIEnterRunningPayload(BaseModel):
    """
    Schema for updating TaskInstance to 'RUNNING' state with minimal required fields.
    """

    state: Annotated[Literal["running"] | None, Field(title="State")] = "running"
    hostname: Annotated[str, Field(title="Hostname")]
    unixname: Annotated[str, Field(title="Unixname")]
    pid: Annotated[int, Field(title="Pid")]
    start_date: Annotated[datetime, Field(title="Start Date")]


class TIHeartbeatInfo(BaseModel):
    """
    Schema for TaskInstance heartbeat endpoint.
    """

    hostname: Annotated[str, Field(title="Hostname")]
    pid: Annotated[int, Field(title="Pid")]


class TIRescheduleStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to a up_for_reschedule state.
    """

    state: Annotated[Literal["up_for_reschedule"] | None, Field(title="State")] = "up_for_reschedule"
    end_date: Annotated[datetime, Field(title="End Date")]
    reschedule_date: Annotated[datetime, Field(title="Reschedule Date")]


class TISuccessStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to success state.
    """

    state: Annotated[Literal["success"] | None, Field(title="State")] = "success"
    end_date: Annotated[datetime, Field(title="End Date")]
    task_outlets: Annotated[list[AssetProfile] | None, Field(title="Task Outlets")] = None
    outlet_events: Annotated[list | None, Field(title="Outlet Events")] = None


class TITargetStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to a target state, excluding terminal and running states.
    """

    state: IntermediateTIState


class TerminalTIState(str, Enum):
    """
    States that a Task Instance can be in that indicate it has reached a terminal state.
    """

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    REMOVED = "removed"
    FAIL_WITHOUT_RETRY = "fail_without_retry"


class ValidationError(BaseModel):
    loc: Annotated[list[str | int], Field(title="Location")]
    msg: Annotated[str, Field(title="Message")]
    type: Annotated[str, Field(title="Error Type")]


class VariablePostBody(BaseModel):
    """
    Request body schema for creating variables.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    value: Annotated[str | None, Field(title="Value")] = None
    description: Annotated[str | None, Field(title="Description")] = None


class VariableResponse(BaseModel):
    """
    Variable schema for responses with fields that are needed for Runtime.
    """

    key: Annotated[str, Field(title="Key")]
    value: Annotated[str | None, Field(title="Value")] = None


class XComResponse(BaseModel):
    """
    XCom schema for responses with fields that are needed for Runtime.
    """

    key: Annotated[str, Field(title="Key")]
    value: Annotated[Any, Field(title="Value")]


class BundleInfo(BaseModel):
    name: str
    version: str | None = None


class TaskInstance(BaseModel):
    """
    Schema for TaskInstance model with minimal required fields needed for Runtime.
    """

    id: Annotated[UUID, Field(title="Id")]
    task_id: Annotated[str, Field(title="Task Id")]
    dag_id: Annotated[str, Field(title="Dag Id")]
    run_id: Annotated[str, Field(title="Run Id")]
    try_number: Annotated[int, Field(title="Try Number")]
    map_index: Annotated[int, Field(title="Map Index")] = -1
    hostname: Annotated[str | None, Field(title="Hostname")] = None


class DagRun(BaseModel):
    """
    Schema for DagRun model with minimal required fields needed for Runtime.
    """

    dag_id: Annotated[str, Field(title="Dag Id")]
    run_id: Annotated[str, Field(title="Run Id")]
    logical_date: Annotated[datetime, Field(title="Logical Date")]
    data_interval_start: Annotated[datetime | None, Field(title="Data Interval Start")] = None
    data_interval_end: Annotated[datetime | None, Field(title="Data Interval End")] = None
    start_date: Annotated[datetime, Field(title="Start Date")]
    end_date: Annotated[datetime | None, Field(title="End Date")] = None
    run_type: DagRunType
    conf: Annotated[dict[str, Any] | None, Field(title="Conf")] = None


class HTTPValidationError(BaseModel):
    detail: Annotated[list[ValidationError] | None, Field(title="Detail")] = None


class TIRunContext(BaseModel):
    """
    Response schema for TaskInstance run context.
    """

    dag_run: DagRun
    max_tries: Annotated[int, Field(title="Max Tries")]
    variables: Annotated[list[VariableResponse] | None, Field(title="Variables")] = None
    connections: Annotated[list[ConnectionResponse] | None, Field(title="Connections")] = None


class TITerminalStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to a terminal state (e.g., SUCCESS or FAILED).
    """

    state: TerminalTIState
    end_date: Annotated[datetime, Field(title="End Date")]
