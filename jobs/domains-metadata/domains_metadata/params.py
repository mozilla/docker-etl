import click

destination_project = click.option(
    "--destination-project",
    required=True,
    type=str,
    help="the GCP project to use for writing data to",
)


destination_table = click.option(
    "--destination-table-id",
    required=True,
    type=str,
    help="the table id to append data to, e.g. `projectid.dataset.table`",

)
