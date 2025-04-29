import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence, cast

import google.auth
from google.cloud import bigquery


def get_client(bq_project_id: str) -> bigquery.Client:
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )

    return bigquery.Client(credentials=credentials, project=bq_project_id)


@dataclass
class RangePartition:
    field: str
    start: int
    end: int
    interval: int = 1


class BigQuery:
    def __init__(self, client: bigquery.Client, default_dataset_id: str, write: bool):
        self.client = client
        self.default_dataset_id = default_dataset_id
        self.write = write

    def get_dataset(self, dataset_id: Optional[str]) -> str:
        if dataset_id is None:
            return self.default_dataset_id
        return dataset_id

    def get_table_id(
        self, dataset_id: Optional[str], table: bigquery.Table | str
    ) -> bigquery.Table | str:
        if isinstance(table, bigquery.Table):
            return table

        if "." in table:
            return table

        dataset_id = self.get_dataset(dataset_id)
        return f"{self.client.project}.{dataset_id}.{table}"

    def ensure_table(
        self,
        table_id: str,
        schema: Iterable[bigquery.SchemaField],
        recreate: bool = False,
        dataset_id: Optional[str] = None,
        partition: Optional[RangePartition] = None,
    ) -> bigquery.Table:
        table = bigquery.Table(self.get_table_id(dataset_id, table_id), schema=schema)
        if partition:
            table.range_partitioning = bigquery.table.RangePartitioning(
                bigquery.table.PartitionRange(
                    partition.start, partition.end, partition.interval
                ),
                field=partition.field,
            )

        if self.write:
            if recreate:
                self.client.delete_table(table, not_found_ok=True)
            self.client.create_table(table, exists_ok=True)
        return table

    def write_table(
        self,
        table: bigquery.Table | str,
        schema: list[bigquery.SchemaField],
        rows: Sequence[Mapping[str, Any]],
        overwrite: bool,
        dataset_id: Optional[str] = None,
    ) -> None:
        table = self.get_table_id(dataset_id, table)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            write_disposition="WRITE_APPEND" if not overwrite else "WRITE_TRUNCATE",
        )

        if self.write:
            job = self.client.load_table_from_json(
                cast(Iterable[dict[str, Any]], rows),
                table,
                job_config=job_config,
            )
            job.result()
            logging.info(f"Wrote {len(rows)} records into {table}")
        else:
            logging.info(f"Skipping writes, would have written {len(rows)} to {table}")
            for row in rows:
                logging.debug(f"  {row}")

    def insert_rows(
        self,
        table: str | bigquery.Table,
        rows: Sequence[Mapping[str, Any]],
        dataset_id: Optional[str] = None,
    ) -> None:
        table = self.get_table_id(dataset_id, table)

        if self.write:
            errors = self.client.insert_rows_json(table, rows)
            if errors:
                logging.error(errors)
        else:
            logging.info(f"Skipping writes, would have written {len(rows)} to {table}")
            for row in rows:
                logging.debug(f"  {row}")

    def query(
        self,
        query: str,
        dataset_id: Optional[str] = None,
        parameters: Optional[Sequence[bigquery.query._AbstractQueryParameter]] = None,
    ) -> bigquery.table.RowIterator:
        """Run a query

        Note that this can't prevent writes in the case that the SQL does writes"""

        job_config = bigquery.QueryJobConfig(
            default_dataset=f"{self.client.project}.{self.get_dataset(dataset_id)}"
        )
        if parameters is not None:
            job_config.query_parameters = parameters

        logging.debug(query)
        return self.client.query(query, job_config=job_config).result()

    def delete_table(
        self, table: bigquery.Table | str, not_found_ok: bool = False
    ) -> None:
        return self.client.delete_table(table, not_found_ok=not_found_ok)
