import json
import os
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
import looker_sdk
import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, pubsub_v1
from looker_sdk import methods40 as methods
from looker_sdk.sdk.api40 import models


def setup_sdk(client_id, client_secret, instance) -> methods.Looker40SDK:
    os.environ["LOOKERSDK_BASE_URL"] = instance
    os.environ["LOOKERSDK_API_VERSION"] = "4.0"
    os.environ["LOOKERSDK_VERIFY_SSL"] = "true"
    os.environ["LOOKERSDK_TIMEOUT"] = "9000"
    os.environ["LOOKERSDK_CLIENT_ID"] = client_id
    os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret

    return looker_sdk.init40()


@click.group()
@click.option("--client_id", "--client-id", envvar="LOOKER_CLIENT_ID", required=True)
@click.option(
    "--client_secret",
    "--client-secret",
    envvar="LOOKER_CLIENT_SECRET",
    required=True,
)
@click.option(
    "--instance_uri", "--instance-uri", envvar="LOOKER_INSTANCE_URI", required=True
)
@click.pass_context
def cli(ctx: dict, client_id: str, client_secret: str, instance_uri: str):
    sdk = setup_sdk(client_id, client_secret, instance_uri)
    ctx.ensure_object(dict)
    ctx.obj["SDK"] = sdk


@cli.command()
@click.option(
    "--project",
    help="Looker project name",
    multiple=True,
    default=["spoke-default", "looker-hub"],
)
@click.option(
    "--inactive_days",
    "--inactive-days",
    help="Delete branches that haven't been updated within the last n days",
    default=180,
)
@click.option(
    "--exclude",
    multiple=True,
    help="Branches to exclude from deletion",
    default=[
        "main",
        "master",
        "main-dev",
        "prod",
        "main-validation",
        "main-stage",
        "base",
    ],
)
@click.pass_context
def delete_branches(ctx, project, inactive_days, exclude):
    sdk = ctx.obj["SDK"]
    date_cutoff = datetime.now().replace(tzinfo=timezone.utc) - timedelta(
        days=inactive_days
    )

    # switch to dev mode
    sdk.update_session(models.WriteApiSession(workspace_id="dev"))

    for lookml_project in project:
        branches = sdk.all_git_branches(project_id=lookml_project)

        for branch in branches:
            commit_date = datetime.fromtimestamp(branch.commit_at, timezone.utc)

            if (
                commit_date < date_cutoff
                and not branch.name.startswith("dev")
                and branch.name not in exclude
            ):
                print(
                    f"{branch.name} in {lookml_project}, last commit on {commit_date}"
                )

                sdk.delete_git_branch(
                    project_id=lookml_project, branch_name=branch.name
                )


@cli.group()
@click.option(
    "--destination_table",
    "--destination-table",
    help="BigQuery destination table",
    required=True,
)
@click.option(
    "--date",
    help="Date to associate results with",
    required=True,
)
@click.pass_context
def analyze(ctx, destination_table, date):
    """Looker usage analysis related commands."""
    ctx.ensure_object(dict)  # Ensure context obj is a dict
    ctx.obj["destination_table"] = destination_table
    ctx.obj["date"] = datetime.strptime(date, "%Y-%m-%d")


@analyze.command(name="projects")
@click.pass_context
def analyze_projects(ctx):
    destination_table = ctx.obj.get("destination_table")
    date = ctx.obj.get("date")
    _henry_analyze(["analyze", "projects"], destination_table, date)


@analyze.command(name="explores")
@click.pass_context
def analyze_explores(ctx):
    destination_table = ctx.obj.get("destination_table")
    date = ctx.obj.get("date")
    _henry_analyze(["analyze", "explores"], destination_table, date)


@analyze.command(name="models")
@click.pass_context
def analyze_models(ctx):
    destination_table = ctx.obj.get("destination_table")
    date = ctx.obj.get("date")
    _henry_analyze(["analyze", "models"], destination_table, date)


@analyze.command(name="unused-explores")
@click.pass_context
def unused_explores(ctx):
    destination_table = ctx.obj.get("destination_table")
    date = ctx.obj.get("date")
    _henry_analyze(["vacuum", "explores"], destination_table, date)


def _henry_analyze(cmd, destination_table, date):
    client = bigquery.Client()
    tmp_config = _henry_config_file()

    file_prefix = "_".join(cmd)
    subprocess.run(
        ["henry"]
        + cmd
        + ["--section", "Looker", "--config-file", str(tmp_config), "--save"]
    )

    output_csv = [f for f in Path(".").rglob(f"{file_prefix}*") if f.is_file()][0]

    df = pd.read_csv(output_csv)
    df["submission_date"] = pd.to_datetime(date)
    df.to_csv(output_csv, index=False)

    partition_str = str(date.strftime("%Y%m%d"))
    table_id = f"{destination_table}${partition_str}"

    try:
        client.get_table(destination_table)
    except NotFound:
        print(f"Table {destination_table} not found. Creating...")
        schema = [
            bigquery.SchemaField("submission_date", "DATE"),
        ]
        table = bigquery.Table(destination_table, schema=schema)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="submission_date",
        )

        table = client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )

    with open(output_csv, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )

    load_job.result()

    print(f"Loaded {load_job.output_rows} rows into {table_id}.")


def _henry_config_file():
    """Generate the config file required to run henry."""
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
        config = f"""
        [Looker]
        base_url={os.environ["LOOKERSDK_BASE_URL"]}
        client_id={os.environ["LOOKERSDK_CLIENT_ID"]}
        client_secret={os.environ["LOOKERSDK_CLIENT_SECRET"]}
        verify_ssl=True
        """
        tmp_file.write(config)
        print("Temporary config file created")
    return tmp_file.name


def _disable_user_in_looker(sdk: methods.Looker40SDK, user_email: str) -> None:
    """Find and disable a user in Looker by email address."""
    users = sdk.search_users(email=user_email)

    if not users:
        print(f"No Looker user found with email: {user_email}")
        return

    user = users[0]

    if user.is_disabled:
        return

    sdk.update_user(user.id, models.WriteUser(is_disabled=True))
    print(f"Disabled user {user_email} (ID: {user.id}) in Looker")


@cli.command()
@click.option(
    "--subscription-id",
    "--subscription_id",
    envvar="LOOKER_PUBSUB_SUBSCRIPTION_ID",
    required=True,
    help="Full PubSub subscription path (projects/{project}/subscriptions/{name})",
)
@click.option(
    "--max-messages",
    "--max_messages",
    default=1000,
    help="Maximum number of messages to pull per run",
)
@click.pass_context
def disable_exited_employees(ctx, subscription_id, max_messages):
    """Disable Looker accounts for offboarded employees via PubSub events.

    Expected message format:
    {
        "event_type": "employee_exit",
        "event_time": "2026-01-26T00:00:00Z",
        "employee_email": "user@mozilla.com",
        "employee_name": "Jane Doe",
        "publish_time": "2026-01-28T19:02:25.123456Z",
        "event_data": {
            "manager_name": "Jane Manager",
            "manager_email": "jmanager@mozilla.com"
        }
    }

    Only messages with event_type "employee_exit" trigger a disable.
    Messages are acknowledged on success and nacked on error so they can be retried.
    """
    sdk = ctx.obj["SDK"]
    subscriber = pubsub_v1.SubscriberClient()

    with subscriber:
        response = subscriber.pull(
            request={
                "subscription": subscription_id,
                "max_messages": max_messages,
            }
        )

        if not response.received_messages:
            print("No messages to process")
            return

        ack_ids = []
        nack_ids = []

        for received_message in response.received_messages:
            message = received_message.message
            try:
                data = json.loads(message.data.decode("utf-8"))
                event_type = data.get("event_type")
                employee_email = data.get("employee_email")

                if event_type != "employee_exit":
                    ack_ids.append(received_message.ack_id)
                    continue

                print(f"Processing employee exit for: {employee_email}")

                if not employee_email:
                    print("Message missing employee_email field, skipping")
                    ack_ids.append(received_message.ack_id)
                    continue

                _disable_user_in_looker(sdk, employee_email)
                ack_ids.append(received_message.ack_id)

            except Exception as e:
                print(f"Error processing message: {e}")
                nack_ids.append(received_message.ack_id)

        if ack_ids:
            subscriber.acknowledge(
                request={"subscription": subscription_id, "ack_ids": ack_ids}
            )

        if nack_ids:
            # Reset deadline to 0 so messages are redelivered immediately
            subscriber.modify_ack_deadline(
                request={
                    "subscription": subscription_id,
                    "ack_ids": nack_ids,
                    "ack_deadline_seconds": 0,
                }
            )

        print(
            f"Done: {len(ack_ids)} message(s) succeeded, {len(nack_ids)} message(s) failed"
        )


if __name__ == "__main__":
    cli()
