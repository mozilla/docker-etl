import uuid
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional, MutableMapping
from urllib.parse import urljoin
from enum import Enum
from typing import Any
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field
from bugdantic.bugzilla import QueryParams


type Json = Mapping[str, Json] | Sequence[Json] | str | int | float | bool | None


class RunStatus(str, Enum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    timed_out = "timed_out"


TERMINAL_STATUSES = {RunStatus.succeeded, RunStatus.failed, RunStatus.timed_out}


class ArtifactRef(BaseModel):
    name: str
    size: int
    content_type: str | None = None


class RunSummary(BaseModel):
    status: str
    error: str | None = None
    findings: dict[str, Json] = Field(default_factory=dict)


class AgentDescriptor(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any]


class RunRef(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: UUID
    agent: str
    status: RunStatus


class RunDoc(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: UUID
    agent: str
    status: RunStatus
    inputs: dict[str, Json]
    created_at: datetime
    updated_at: datetime
    execution_name: str | None = None
    results_prefix: str
    summary: RunSummary | None = None
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    error: str | None = None


class CreateRequest(BaseModel):
    agent: str


class AutowebcompatReproRequest(CreateRequest):
    bug_data: str
    bug_id: Optional[int] = None
    model: Optional[str] = None
    max_turns: Optional[int] = None
    effort: Optional[str] = None
    agent: str = "autowebcompat-repro"


class HackbotAgentResult(BaseModel):
    num_turns: int
    total_cost_usd: float | None = None


class BaseSummary(BaseModel):
    status: str
    error: Optional[str]
    # Subclasses will usually have something that inherits from HackbotAgentResult
    # findings: HackbotAgentResult


@dataclass
class HackbotConfig:
    base_url: str
    api_key: Optional[str] = None
    request_timeout: Optional[int] = 60
    allow_writes: bool = False
    # Number of times to retry a request if there's a 503 error
    max_retries: int = 1


class Hackbot:
    def __init__(self, config: HackbotConfig):
        self.config = config
        headers = (
            {"X-API-Key": self.config.api_key}
            if self.config.api_key is not None
            else None
        )

        self.client = httpx.Client(
            http2=True, timeout=config.request_timeout, headers=headers
        )

    def _request(
        self,
        method: Literal["GET"] | Literal["POST"],
        path: str,
        params: Optional[QueryParams] = None,
        headers: Optional[dict[str, str]] = None,
        json_body: Optional[MutableMapping[str, Json]] = None,
    ) -> Optional[Json]:
        url = urljoin(self.config.base_url, path)

        if self.config.allow_writes or method in {"GET", "OPTIONS", "HEAD"}:
            retry = 0
            if self.config.max_retries < 0:
                raise ValueError("max_retries must be at least 0")
            response = None
            while retry <= self.config.max_retries:
                retry += 1
                response = self.client.request(
                    method, url, params=params, headers=headers, json=json_body
                )
                if response.status_code != 503:
                    break
            assert response is not None
            try:
                response.raise_for_status()
            except Exception as e:
                msg = f"Request failed\n{response.text}"
                logging.error(msg)
                raise e
            return response.json()
        else:
            logging.info(f"""Not updating, would send {method} request to {path} with body:
{json.dumps(json_body)}
===
""")
        return {}

    def create_run(
        self,
        request: CreateRequest,
    ) -> RunRef:
        body = request.model_dump(mode="json")
        body.pop("agent")
        if self.config.allow_writes:
            resp = self._request(
                "POST", f"/agents/{request.agent}/runs", json_body=body
            )
            return RunRef.model_validate(resp)
        else:
            logging.info(
                f"Would have created run for agent {request.agent} with body {json.dumps(body)}"
            )
            return RunRef(
                run_id=uuid.uuid4(), agent=request.agent, status=RunStatus.pending
            )

    def poll_run(self, run_uuid: UUID) -> tuple[RunDoc, bool]:
        run_data = RunDoc.model_validate(self._request("GET", f"/runs/{run_uuid.hex}"))
        complete = run_data.status in TERMINAL_STATUSES
        return run_data, complete
