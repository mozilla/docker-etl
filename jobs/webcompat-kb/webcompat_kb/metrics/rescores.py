import logging
import os
import pathlib
import tomllib
from typing import Any, Mapping, MutableMapping

import tomli_w
from pydantic import BaseModel, RootModel
from ..bqhelpers import DatasetId, SchemaId, SchemaType


class RescoreData(BaseModel):
    reason: str
    routine_updates: list[str]
    stage: bool = False


class RescoreFile(RootModel):
    root: Mapping[str, RescoreData]


class Rescore:
    def __init__(
        self, name: str, reason: str, routine_updates: list[SchemaId], stage: bool
    ):
        self.name = name
        self.reason = reason
        self.routine_updates = routine_updates
        self.stage = stage

    def staging_schema_id(
        self, reference_type: SchemaType, schema_id: SchemaId
    ) -> SchemaId:
        prefix = f"rescore_{self.name}"
        if reference_type == SchemaType.routine:
            prefix = prefix.upper()
        return SchemaId(
            schema_id.project, schema_id.dataset, f"{prefix}_{schema_id.name}"
        )

    def archive_schema_id(
        self, reference_type: SchemaType, schema_id: SchemaId
    ) -> SchemaId:
        suffix = f"before_rescore_{self.name}"
        if reference_type == SchemaType.routine:
            suffix = suffix.upper()
        return SchemaId(
            schema_id.project,
            f"{schema_id.dataset}_archive",
            f"{schema_id.name}_{suffix}",
        )

    def delta_schema_id(self, dataset: DatasetId) -> SchemaId:
        return SchemaId(
            dataset.project,
            dataset.dataset,
            f"rescore_{self.name}_delta",
        )

    def staging_routine_ids(self) -> Mapping[SchemaId, SchemaId]:
        return {
            schema_id: self.staging_schema_id(SchemaType.routine, schema_id)
            for schema_id in self.routine_updates
        }

    def archive_routine_ids(self) -> Mapping[SchemaId, SchemaId]:
        return {
            schema_id: self.archive_schema_id(SchemaType.routine, schema_id)
            for schema_id in self.routine_updates
        }


def _path(root_path: os.PathLike) -> pathlib.Path:
    path = pathlib.Path(root_path) / "metrics" / "rescores.toml"
    return path.absolute()


def _load_data(path: pathlib.Path) -> MutableMapping[str, Any]:
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return data


def load(root_path: os.PathLike, default_dataset: DatasetId) -> Mapping[str, Rescore]:
    path = _path(root_path)
    data = _load_data(path)

    rescores = {}
    for name, rescore_data in RescoreFile.model_validate(data).root.items():
        routine_schema_ids = [
            SchemaId.from_str(name, default_dataset.project, default_dataset.dataset)
            for name in rescore_data.routine_updates
        ]
        rescores[name] = Rescore(
            name, rescore_data.reason, routine_schema_ids, rescore_data.stage
        )

    return rescores


def update(root_path: os.PathLike, rescore: Rescore, write: bool) -> None:
    path = _path(root_path)
    data = _load_data(path)

    data[rescore.name] = RescoreData(
        reason=rescore.reason,
        routine_updates=[str(item) for item in rescore.routine_updates],
        stage=rescore.stage,
    ).model_dump(exclude_defaults=True)
    if write:
        with open(path, "wb") as f:
            tomli_w.dump(data, f, indent=2)
    else:
        logging.info(
            f"Would write rescores file {path}:\n{tomli_w.dumps(data, indent=2)}"
        )
