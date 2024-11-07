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
#   version:   0.0.0

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from msgspec import Struct


class TIEnterRunningPayload(Struct):
    """
    Schema for updating TaskInstance to 'RUNNING' state with minimal required fields.
    """

    hostname: str
    unixname: str
    pid: int
    start_date: datetime
    state: Literal["running"] | None = "running"


class TIHeartbeatInfo(Struct):
    """
    Schema for TaskInstance heartbeat endpoint.
    """

    hostname: str
    pid: int


class State(Enum):
    REMOVED = "removed"
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    RESTARTING = "restarting"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    DEFERRED = "deferred"


class TITargetStatePayload(Struct):
    """
    Schema for updating TaskInstance to a target state, excluding terminal and running states.
    """

    state: State


class State1(Enum):
    FAILED = "failed"
    SUCCESS = "success"
    SKIPPED = "skipped"


class TITerminalStatePayload(Struct):
    """
    Schema for updating TaskInstance to a terminal state (e.g., SUCCESS or FAILED).
    """

    state: State1
    end_date: datetime


class TaskInstanceState(str, Enum):
    """
    All possible states that a Task Instance can be in.

    Note that None is also allowed, so always use this in a type hint with Optional.
    """

    REMOVED = "removed"
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    RESTARTING = "restarting"
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    DEFERRED = "deferred"
