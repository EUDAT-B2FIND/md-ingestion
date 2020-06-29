import click
import pathlib

from .command import Harvest, Map, Upload
from .exceptions import UserInfo


import logging


CONTEXT_OBJ = dict()
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'], obj=CONTEXT_OBJ)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option('--debug', is_flag=True)
@click.option('--dry-run', is_flag=True, help='use dry run mode')
@click.option('--list', '-l', default='ingestion_list', help='Filename with list of source.')
@click.option('--outdir', '-o', default='oaidata',
              help='The absolute root dir in which all harvested files will be saved.')
@click.pass_context
def cli(ctx, debug, dry_run, list, outdir):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj['list'] = list
    ctx.obj['dry_run'] = dry_run

    out = pathlib.Path(outdir)
    if not out.is_absolute():
        out = pathlib.Path.cwd().joinpath(out)
    ctx.obj['outdir'] = out.absolute().as_posix()

    if debug:
        logging.basicConfig(filename='out.log', level=logging.DEBUG)
    else:
        logging.basicConfig(filename='out.log', level=logging.INFO)


@cli.command()
@click.option('--community', '-c', required=True, help='Community')
@click.option('--url', help='Source URL')
@click.option('--verb',
              help='Requests defined in OAI-PMH: ListRecords (default) or ListIdentifers.')
@click.option('--mdprefix', help='Metadata prefix')
@click.option('--mdsubset', help='Subset')
@click.option('--fromdate', type=click.DateTime(formats=["%Y-%m-%d"]),
              help='Filter by date.')
@click.option('--limit', type=int, help='Limit')
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.pass_context
def harvest(ctx, community, url, verb, mdprefix, mdsubset, fromdate, limit, insecure):
    try:
        cmd = Harvest(
            sources=ctx.obj['list'],
            community=community,
            url=url,
            verb=verb,
            mdprefix=mdprefix,
            mdsubset=mdsubset,
            outdir=ctx.obj['outdir'],
            verify=not insecure,
        )
        if fromdate:
            fromdate = str(fromdate.date())
        cmd.harvest(fromdate=fromdate, limit=limit, dry_run=ctx.obj['dry_run'])
    except UserInfo as e:
        click.echo(f'{e}')
    except Exception as e:
        logging.critical(f"harvest: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--community', '-c', help='Community')
@click.option('--url', help='Source URL')
@click.option('--mdprefix', help='Metadata prefix')
@click.option('--mdsubset', help='Subset')
@click.pass_context
def map(ctx, community, url, mdprefix, mdsubset):
    try:
        map = Map(
            sources=ctx.obj['list'],
            community=community,
            url=url,
            mdprefix=mdprefix,
            mdsubset=mdsubset,
            outdir=ctx.obj['outdir'],)
        map.run()
    except Exception as e:
        logging.critical(f"map: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--community', '-c', help='Community')
@click.option('--iphost', '-i', help='IP address of CKAN instance')
@click.option('--auth', help='CKAN API key')
@click.pass_context
def upload(ctx, community, iphost, auth):
    try:
        upload = Upload(outdir=ctx.obj['outdir'], sources=ctx.obj['list'], iphost=iphost, auth=auth)
        upload.upload(community)
    except Exception as e:
        logging.critical(f"upload: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


if __name__ == '__main__':
    cli(obj={})
