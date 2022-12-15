import click
from dotenv import load_dotenv

from domains_metadata.categories import load_cf_categories, load_cf_domain_categories
from domains_metadata.params import destination_table, destination_project


@click.group()
def cli():
    """Create the CLI."""
    load_dotenv()
    pass


@cli.command()
@destination_project
@destination_table
def load_categories(
    destination_project,
    destination_table_id,
):
    load_cf_categories(destination_project, destination_table_id)


@cli.command()
@destination_project
@destination_table
def load_domain_categories(
    destination_project,
    destination_table_id,
):
    load_cf_domain_categories(destination_project, destination_table_id)


if __name__ == "__main__":
    cli()
