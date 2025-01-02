import os
from datetime import datetime, timedelta, timezone

import click
import looker_sdk
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


if __name__ == "__main__":
    cli()
