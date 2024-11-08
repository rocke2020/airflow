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

import uuid
from typing import TYPE_CHECKING, Any

import httpx
import methodtools
import msgspec
import structlog
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import (
    State1 as TerminalState,
    TaskInstanceState,
    TIEnterRunningPayload,
    TITerminalStatePayload,
)
from airflow.utils.net import get_hostname
from airflow.utils.platform import getuser

if TYPE_CHECKING:
    from datetime import datetime

log = structlog.get_logger(logger_name=__name__)

__all__ = [
    "Client",
    "TaskInstanceOperations",
]


def raise_on_4xx_5xx(response: httpx.Response):
    return response.raise_for_status()


# Py 3.11+ version
def raise_on_4xx_5xx_with_note(response: httpx.Response):
    try:
        return response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if TYPE_CHECKING:
            assert hasattr(e, "add_note")
        e.add_note(
            f"Correlation-id={response.headers.get('correlation-id', None) or response.request.headers.get('correlation-id', 'no-correlction-id')}"
        )
        raise


if hasattr(BaseException, "add_note"):
    # Py 3.11+
    raise_on_4xx_5xx = raise_on_4xx_5xx_with_note


def add_correlation_id(request: httpx.Request):
    request.headers["correlation-id"] = str(uuid7())


class TaskInstanceOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
        self.client = client

    def start(self, id: uuid.UUID, pid: int, when: datetime):
        """Tell the API server that this TI has started running."""
        body = TIEnterRunningPayload(pid=pid, hostname=get_hostname(), unixname=getuser(), start_date=when)

        self.client.patch(f"task_instance/{id}/state", content=self.client.encoder.encode(body))

    def finish(self, id: uuid.UUID, state: TaskInstanceState, when: datetime):
        """Tell the API server that this TI has reached a terminal state."""
        body = TITerminalStatePayload(end_date=when, state=TerminalState(state))

        self.client.patch(f"task_instance/{id}/state", content=self.client.encoder.encode(body))

    def heartbeat(self, id: uuid.UUID):
        self.client.put(f"task_instance/{id}/heartbeat")


class BearerAuth(httpx.Auth):
    def __init__(self, token: str):
        self.token: str = token

    def auth_flow(self, request: httpx.Request):
        if self.token:
            request.headers["Authorization"] = "Bearer " + self.token
        yield request


def noop_handler(request: httpx.Request) -> httpx.Response:
    log.debug("Dry-run request", method=request.method, path=request.url.path)
    return httpx.Response(200, json={"text": "Hello, world!"})


class Client(httpx.Client):
    encoder: msgspec.json.Encoder

    def __init__(self, *, base_url: str | None, dry_run: bool = False, token: str, **kwargs: Any):
        if (not base_url) ^ dry_run:
            raise ValueError(f"Can only specify one of {base_url=} or {dry_run=}")
        auth = BearerAuth(token)

        self.encoder = msgspec.json.Encoder()
        if dry_run:
            # If dry run is requests, install a no op handler so that simple tasks can "heartbeat" using a
            # real client, but just don't make any HTTP requests
            kwargs["transport"] = httpx.MockTransport(noop_handler)
            kwargs["base_url"] = "dry-run://server"
        else:
            kwargs["base_url"] = base_url
        super().__init__(
            auth=auth,
            headers={"airflow-api-version": "2024-07-30"},
            event_hooks={"response": [raise_on_4xx_5xx], "request": [add_correlation_id]},
            **kwargs,
        )

    # We "group" or "namespace" operations by what they operate on, rather than a flat namespace with all
    # methods on one object prefixed with the object type (`.task_instances.update` rather than
    # `task_instance_update` etc.)

    @methodtools.lru_cache()  # type: ignore[misc]
    @property
    def task_instances(self) -> TaskInstanceOperations:
        """Operations related to TaskInstances."""
        return TaskInstanceOperations(self)
