import json
import logging
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any, Literal, Optional, Annotated
from urllib.parse import urljoin


import httpx
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

Json = Mapping[str, "Json"] | Sequence["Json"] | str | int | float | bool | None

DEFAULT_BASE_URL = "https://sql.telemetry.mozilla.org/"


class RedashDataSourcesResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    name: str
    type: str
    description: Optional[str]
    syntax: Optional[str]
    paused: int
    pause_reason: Optional[str]
    options: Mapping[str, Any]
    queue_name: str
    scheduled_queue_name: str
    groups: Mapping[str, bool]
    view_only: bool


class RedashUserResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    name: str
    email: str
    profile_image_url: Optional[str] = None
    groups: list[int]
    updated_at: datetime
    created_at: datetime
    disabled_at: Optional[datetime] = None
    is_disabled: bool
    active_at: Optional[datetime] = None
    is_invitation_pending: bool
    is_email_verified: bool
    auth_type: str


class RedashVisualizationResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    type: str
    name: str
    description: Optional[str]
    options: dict[str, Any]
    updated_at: datetime
    created_at: datetime


class RedashParameterBase(BaseModel):
    title: str
    name: str
    type: str


class RedashEnumMultiValuesOptions(BaseModel):
    prefix: str
    suffix: str
    separator: str


class RedashEnumParameter(RedashParameterBase):
    type: Literal["enum"] = "enum"
    enumOptions: Optional[str]
    value: str | list[str]
    multiValuesOptions: Optional[RedashEnumMultiValuesOptions] = Field(
        exclude_if=lambda v: v is None, default=None
    )


class RedashQueryParameter(RedashParameterBase):
    type: Literal["query"] = "query"
    queryId: int
    value: str | list[str] = Field(exclude_if=lambda v: v is None)
    multiValuesOptions: Optional[RedashEnumMultiValuesOptions] = Field(
        exclude_if=lambda v: v is None, default=None
    )


class RedashTextParameter(RedashParameterBase):
    type: Literal["text"] = "text"
    value: str


class RedashNumberParameter(RedashParameterBase):
    type: Literal["number"] = "number"
    value: int | float


class RedashDateParameter(RedashParameterBase):
    type: Literal["date"] = "date"
    value: date

    @field_serializer("value", when_used="always")
    def serialize_value(self, v: date) -> str:
        return v.isoformat()


class RedashDateTimeParameter(RedashParameterBase):
    type: Literal["datetime-local"] = "datetime-local"
    value: datetime

    @field_validator("value", mode="before")
    @classmethod
    def parse_value(cls, v: Any) -> Any:
        if isinstance(v, str):
            return datetime.strptime(v, "%Y-%m-%d %H:%M")
        return v

    @field_serializer("value", when_used="always")
    def serialize_value(self, v: datetime) -> str:
        return v.strftime("%Y-%m-%d %H:%M")


class RedashDateTimeWithSecondsParameter(RedashParameterBase):
    type: Literal["datetime-with-seconds"] = "datetime-with-seconds"
    value: datetime

    @field_validator("value", mode="before")
    @classmethod
    def parse_value(cls, v: Any) -> Any:
        if isinstance(v, str):
            return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        return v

    @field_serializer("value", when_used="always")
    def serialize_value(self, v: datetime) -> str:
        return v.strftime("%Y-%m-%d %H:%M:%S")


class RedashDateRangeValue(BaseModel):
    start: date
    end: date

    @field_serializer("start", "end", when_used="always")
    def serialize_date(self, v: date) -> str:
        return v.isoformat()


class RedashDateRangeParameter(RedashParameterBase):
    type: Literal["date-range"] = "date-range"
    value: RedashDateRangeValue


class RedashDateTimeRangeValue(BaseModel):
    start: datetime
    end: datetime

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> Any:
        if isinstance(v, str):
            return datetime.strptime(v, "%Y-%m-%d %H:%M")
        return v

    @field_serializer("start", "end", when_used="always")
    def serialize_datetime(self, v: datetime) -> str:
        return v.strftime("%Y-%m-%d %H:%M")


class RedashDateTimeRangeParameter(RedashParameterBase):
    type: Literal["datetime-range"] = "datetime-range"
    value: RedashDateTimeRangeValue


class RedashDateTimeWithSecondsRangeValue(BaseModel):
    start: datetime
    end: datetime

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> Any:
        if isinstance(v, str):
            return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        return v

    @field_serializer("start", "end", when_used="always")
    def serialize_datetime(self, v: datetime) -> str:
        return v.strftime("%Y-%m-%d %H:%M:%S")


class RedashDateTimeWithSecondsRangeParameter(RedashParameterBase):
    type: Literal["datetime-range-with-seconds"] = "datetime-range-with-seconds"
    value: RedashDateTimeWithSecondsRangeValue


class RedashOptions(BaseModel):
    parameters: list[
        Annotated[
            RedashTextParameter
            | RedashNumberParameter
            | RedashEnumParameter
            | RedashQueryParameter
            | RedashDateParameter
            | RedashDateTimeParameter
            | RedashDateTimeWithSecondsParameter
            | RedashDateRangeParameter
            | RedashDateTimeRangeParameter
            | RedashDateTimeWithSecondsRangeParameter,
            Field(discriminator="type"),
        ]
    ]


class RedashQueryResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    latest_query_data_id: Optional[int] = None
    name: str
    description: Optional[str] = None
    query: str
    query_hash: str
    schedule: Optional[Any] = None
    api_key: str
    is_archived: bool
    is_draft: bool
    updated_at: datetime
    created_at: datetime
    data_source_id: int
    options: RedashOptions
    version: int
    tags: list[str]
    is_safe: bool
    user: RedashUserResponse
    last_modified_by: Optional[RedashUserResponse] = None
    visualizations: list[RedashVisualizationResponse]
    is_favorite: bool


class RedashClientException(Exception):
    pass


class RedashClient(object):
    def __init__(
        self,
        api_key: str,
        default_data_source: Optional[str | int],
        base_url: str = DEFAULT_BASE_URL,
        allow_updates: bool = True,
    ):
        self._api_key = api_key
        self.base_url = base_url
        self._headers = {"Authorization": f"Key {self._api_key}"}
        self._allow_updates = allow_updates
        if isinstance(default_data_source, str):
            default_data_source = self.get_data_source_id(default_data_source)
        self.default_data_source = default_data_source

    def _make_api_request(
        self,
        method: str,
        url_path: str,
        body: Optional[Json | BaseModel] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Json:
        url = urljoin(self.base_url, f"api/{url_path}")
        req_headers = self._headers.copy()
        if headers is not None:
            req_headers.update(headers)
        if method not in {"GET", "HEAD"} and not self._allow_updates:
            raise RedashClientException(
                f"Tried to {method} to {url} with allow_updates=False"
            )

        if isinstance(body, BaseModel):
            body = body.model_dump()

        response = httpx.request(method, url, json=body, headers=req_headers)

        if response.status_code != 200:
            short_msg = f"{method} request to {url}: {response.status_code}\nResponse body:\n{response.content.decode('utf8')}"
            if method == "POST" and body:
                msg = f"{short_msg}\nRequest body:\n{json.dumps(body, indent=2)}"
            else:
                msg = short_msg
            logging.error(msg)
            raise RedashClientException(short_msg, response.status_code)
        try:
            json_result = response.json()
        except ValueError as e:
            raise RedashClientException((f"Unable to parse JSON response: {e}"))

        return json_result

    def get_data_source_id(self, data_source_name: str) -> int:
        data_sources = self.get_data_sources()
        for data_source in data_sources:
            if data_source.name == data_source_name:
                return data_source.id
        raise ValueError(f"Failed to find data source {data_source_name}")

    def _data_source_id(self, data_source_id: Optional[str | int]) -> int:
        if data_source_id is None:
            if self.default_data_source is None:
                raise ValueError("Need to provide a data source id")
            return self.default_data_source
        if isinstance(data_source_id, str):
            return self.get_data_source_id(data_source_id)
        return data_source_id

    def get_data_sources(self) -> Sequence[RedashDataSourcesResponse]:
        url_path = "data_sources"
        json_response = self._make_api_request("GET", url_path)
        assert isinstance(json_response, list)
        return [
            RedashDataSourcesResponse.model_validate(item) for item in json_response
        ]

    def create_query(
        self,
        name: str,
        sql_query: str,
        data_source_id: Optional[str] = None,
        description: Optional[str] = None,
        options: Optional[RedashOptions] = None,
    ) -> RedashQueryResponse:
        url_path = "queries"

        query_args: dict[str, Json] = {
            "name": name,
            "query": sql_query,
            "data_source_id": self._data_source_id(data_source_id),
            "description": description,
        }
        if options is not None:
            query_args["options"] = options.model_dump()

        json_result = self._make_api_request("POST", url_path, body=query_args)
        return RedashQueryResponse.model_validate(json_result)

    def get_query(self, query_id: int) -> RedashQueryResponse:
        json_result = self._make_api_request("GET", f"queries/{query_id}")
        return RedashQueryResponse.model_validate(json_result)

    def delete_query(self, query_id: int) -> None:
        self._make_api_request("DELETE", f"queries/{query_id}")

    def update_query(
        self,
        query_id: int,
        name: str,
        sql_query: str,
        data_source_id: Optional[str | int] = None,
        description: Optional[str] = None,
        options: Optional[RedashOptions] = None,
    ) -> RedashQueryResponse:
        query_args: dict[str, Json] = {
            "data_source_id": self._data_source_id(data_source_id),
            "query": sql_query,
            "name": name,
            "description": description,
            "id": query_id,
        }

        if options is not None:
            query_args["options"] = options.model_dump()

        json_result = self._make_api_request(
            "POST", f"queries/{query_id}", body=query_args
        )
        return RedashQueryResponse.model_validate(json_result)

    def fork_query(self, query_id: int) -> RedashQueryResponse:
        json_result = self._make_api_request("POST", f"queries/{query_id}/fork")
        return RedashQueryResponse.model_validate(json_result)
