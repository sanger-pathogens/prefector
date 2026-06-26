import click

from prefector.blocks.cli import blocks_command
from prefector.deployments.cli import deployments_command


@click.group()
@click.option("--debug", is_flag=True, default=False, help="Show full tracebacks on error.")
@click.pass_context
def cli(ctx, debug):
    """Manage Prefect resources from reusable specs."""
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug


cli.add_command(blocks_command)
cli.add_command(deployments_command)

if __name__ == "__main__":
    cli()
