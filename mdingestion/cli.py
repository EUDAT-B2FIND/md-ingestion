import click
import pathlib
from datetime import datetime, timedelta

from .command import List, Harvest, Map, Upload, Purge, Search
from .exceptions import UserInfo


import logging


CONTEXT_OBJ = dict()
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'], obj=CONTEXT_OBJ)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option('--debug', is_flag=True)
@click.option('--silent', is_flag=True, help='silent mode')
@click.option('--dry-run', is_flag=True, help='use dry run mode')
@click.option('--outdir', '-o', default='.',
              help='The root dir for all outputs.')
@click.pass_context
def cli(ctx, debug, silent, dry_run, outdir):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj['dry_run'] = dry_run
    ctx.obj['silent'] = silent

    out = pathlib.Path(outdir)
    if not out.is_absolute():
        out = pathlib.Path.cwd().joinpath(out)
    out.mkdir(parents=True, exist_ok=True)
    ctx.obj['outdir'] = out.absolute().as_posix()
    # logging
    logfile = out.joinpath('b2f.log').as_posix()
    if debug:
        logging.basicConfig(filename=logfile, level=logging.DEBUG)
    else:
        logging.basicConfig(filename=logfile, level=logging.ERROR)


@cli.command()
@click.option('--community', '-c', help='Community')
@click.option('--summary', '-s', is_flag=True, help='Summary')
@click.option('--productive', '-p', is_flag=True, help='Productive')
@click.option('--out', '-o', help='Output as CSV file')
@click.pass_context
def list(ctx, community, summary, productive, out):
    try:
        list = List()
        list.run(name=community, summary=summary,
                 productive=productive, out=out)
    except Exception as e:
        logging.critical(f"list: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--community', '-c', required=True, help='Community')
@click.option('--url', help='Source URL')
@click.option('--fromdate', type=click.DateTime(formats=["%Y-%m-%d"]),
              help='Filter by date.')
@click.option('--clean', is_flag=True, help='Clean output folder before harvesting')
@click.option('--limit', type=int, help='Limit')
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.option('--username', '-u', help='Username for secured OAI server')
@click.option('--password', '-p', help='Password for secured OAI server')
@click.pass_context
def harvest(ctx, community, url, fromdate, clean, limit, insecure, username, password):
    try:
        cmd = Harvest(
            community=community,
            url=url,
            outdir=ctx.obj['outdir'],
            verify=not insecure
        )
        if fromdate:
            fromdate = str(fromdate.date())
        cmd.harvest(fromdate=fromdate, clean=clean, limit=limit,
                    dry_run=ctx.obj['dry_run'], silent=ctx.obj['silent'],
                    username=username, password=password)
    except UserInfo as e:
        click.echo(f'{e}')
    except Exception as e:
        logging.critical(f"harvest: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--community', '-c', required=True, help='Community')
@click.option('--format', default='ckan', help='output format: ckan (default) or b2f')
@click.option('--limit', type=int, help='Limit')
@click.option('--force', is_flag=True, help='force')
@click.option('--linkcheck/--no-linkcheck', default=False, is_flag=True, help='do not check if URLs resolve in validation')
@click.pass_context
def map(ctx, community, format, limit, force, linkcheck):
    try:
        map = Map(
            community=community,
            outdir=ctx.obj['outdir'],)
        map.run(format=format, force=force, linkcheck=linkcheck, limit=limit,
                silent=ctx.obj['silent'])
    except Exception as e:
        logging.critical(f"map: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--community', '-c', required=True, help='Community')
@click.option('--iphost', '-i', required=True, help='IP address of CKAN instance')
@click.option('--auth', required=True, help='CKAN API key')
@click.option('--target', default='ckan', help='Target service: ckan (default)')
@click.option('--limit', type=int, help='Limit')
@click.option('--from', '-f', 'from_', type=int, help='From index')
@click.option('--no-update', is_flag=True, help='do not update existing record')
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.pass_context
def upload(ctx, community, iphost, auth, target, from_, limit, no_update, insecure):
    try:
        upload = Upload(outdir=ctx.obj['outdir'], community=community)
        upload.run(iphost=iphost, auth=auth, target=target, from_=from_, limit=limit,
                   no_update=no_update, verify=not insecure,
                   silent=ctx.obj['silent'])
    except Exception as e:
        logging.critical(f"upload: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--community', '-c', required=True, help='Community')
@click.option('--iphost', '-i', required=True, help='IP address of CKAN instance')
@click.option('--auth', required=True, help='CKAN API key')
@click.option('--fromdate', type=click.DateTime(formats=["%Y-%m-%d"]),
              help='Harvest records not older than given date.')
@click.option('--fromdays', type=int, help='Harvest records not older than given days ago.')
@click.option('--clean', is_flag=True, help='Clean output folder before harvesting')
@click.option('--limit', type=int, help='Limit')
@click.option('--linkcheck/--no-linkcheck', default=False, is_flag=True, help='do not check if URLs resolve in validation')
@click.option('--no-update', is_flag=True, help='do not update existing record')
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.pass_context
def combine(ctx, community, iphost, auth, fromdate, fromdays, clean, limit, linkcheck, no_update, insecure):
    try:
        # harvest
        cmd = Harvest(
            community=community,
            outdir=ctx.obj['outdir'],
            verify=not insecure,
        )
        if fromdays:
            fromdate = datetime.now() - timedelta(days=fromdays)
        if fromdate:
            fromdate = str(fromdate.date())
        cmd.harvest(fromdate=fromdate, clean=clean, limit=limit,
                    dry_run=ctx.obj['dry_run'], silent=ctx.obj['silent'])
        # map
        cmd = Map(
            community=community,
            outdir=ctx.obj['outdir'],)
        cmd.run(format='ckan', force=False, linkcheck=linkcheck, limit=limit,
                silent=ctx.obj['silent'])
        # upload
        upload = Upload(outdir=ctx.obj['outdir'], community=community)
        upload.run(iphost=iphost, auth=auth, target='ckan', from_=None, limit=limit,
                   no_update=no_update, verify=not insecure,
                   silent=ctx.obj['silent'])
    except UserInfo as e:
        click.echo(f'{e}')
    except Exception as e:
        logging.critical(f"combine: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--community', '-c', required=False,
              help='delete all datasets of this community')
@click.option('--dataset', '-d', metavar='DATASET_ID', required=False, help='delete single dataset')
@click.option('--iphost', '-i', required=True, help='IP address of CKAN instance')
@click.option('--auth', required=True, help='CKAN API key')
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.pass_context
def purge(ctx, community, dataset, iphost, auth, insecure):
    try:
        purge = Purge(community=community)
        purge.run(iphost=iphost, dataset=dataset, auth=auth, verify=not insecure, silent=ctx.obj['silent'])
    except Exception as e:
        logging.critical(f"purge: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--community', '-c', help='Community')
@click.option('--iphost', '-i', required=True, help='IP address of CKAN instance', default="b2find.eudat.eu")
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.option('--limit', type=int, help='Limit of shown datasets', default=20)
@click.option('--pattern', help='Search criteria', default="")
@click.pass_context
def search(ctx, community, iphost, insecure, limit, pattern):
    try:
        search = Search(community=community)
        search.run(iphost=iphost, limit=limit, pattern=pattern, verify=not insecure, silent=ctx.obj['silent'])
    except Exception as e:
        logging.critical(f"search: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


if __name__ == '__main__':
    cli(obj={})
