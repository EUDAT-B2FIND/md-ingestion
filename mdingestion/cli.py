import click
import pathlib
from datetime import datetime, timedelta

from .command import List, Harvest, Map, Upload, Purge, Search, CronGen
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
@click.option('--log', default='b2f.log', help='set logfile name')
@click.pass_context
def cli(ctx, debug, silent, dry_run, outdir, log):
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
    logfile = out.joinpath(log).as_posix()
    if debug:
        logging.basicConfig(filename=logfile, level=logging.DEBUG)
    else:
        logging.basicConfig(filename=logfile, level=logging.ERROR)


@cli.command()
@click.option('--verbose', '-v', is_flag=True, help='Verbose, all info')
@click.option('--stat', '-s', is_flag=True, help='Creates repository statistics')
@click.option('--out', '-o', help='Output as CSV file')
@click.pass_context
def list(ctx, verbose, stat, out):
    try:
        list = List()
        list.run(verbose=verbose, stat=stat, out=out)
    except Exception as e:
        logging.critical(f"list: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--repo', '-c', help='Repository')
@click.option('--auth', required=True, help='CKAN API key')
@click.option('--out', '-o', help='Output file')
@click.pass_context
def cron(ctx, repo, auth, out):
    try:
        crongen = CronGen()
        crongen.run(name=repo, auth=auth, out=out)
    except Exception as e:
        logging.critical(f"cron: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--repo', '-c', required=True, help='Repository')
@click.option('--url', help='Source URL')
@click.option('--fromdate', type=click.DateTime(formats=["%Y-%m-%d"]),
              help='Filter by date.')
@click.option('--clean', is_flag=True, help='Clean output folder before harvesting')
@click.option('--limit', type=int, help='Limit')
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.option('--username', '-u', help='Username for secured OAI server')
@click.option('--password', '-p', help='Password for secured OAI server')
@click.pass_context
def harvest(ctx, repo, url, fromdate, clean, limit, insecure, username, password):
    try:
        cmd = Harvest(
            repo=repo,
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
@click.option('--repo', '-c', required=True, help='Repository')
@click.option('--format', default='ckan', help='output format: ckan (default) or b2f')
@click.option('--limit', type=int, help='Limit')
@click.option('--force', is_flag=True, help='force')
@click.option('--linkcheck/--no-linkcheck', default=False, is_flag=True, help='do not check if URLs resolve in validation')
@click.pass_context
def map(ctx, repo, format, limit, force, linkcheck):
    try:
        map = Map(
            repo=repo,
            outdir=ctx.obj['outdir'],)
        map.run(format=format, force=force, linkcheck=linkcheck, limit=limit,
                silent=ctx.obj['silent'])
    except Exception as e:
        logging.critical(f"map: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--repo', '-c', required=True, help='Repository')
@click.option('--iphost', '-i', required=True, help='IP address of CKAN instance')
@click.option('--auth', required=True, help='CKAN API key')
@click.option('--target', default='ckan', help='Target service: ckan (default)')
@click.option('--limit', type=int, help='Limit')
@click.option('--from', '-f', 'from_', type=int, help='From index')
@click.option('--no-update', is_flag=True, help='do not update existing record')
@click.option('--https', '-s', is_flag=True, help='enable upload on https')
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.pass_context
def upload(ctx, repo, iphost, auth, target, from_, limit, no_update, https, insecure):
    try:
        upload = Upload(outdir=ctx.obj['outdir'], repo=repo)
        upload.run(iphost=iphost, auth=auth, target=target, from_=from_, limit=limit,
                   no_update=no_update, verify=not insecure,
                   silent=ctx.obj['silent'], https=https)
    except Exception as e:
        logging.critical(f"upload: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--repo', '-c', required=True, help='Repository')
@click.option('--iphost', '-i', required=True, help='IP address of CKAN instance')
@click.option('--auth', required=True, help='CKAN API key')
@click.option('--fromdate', type=click.DateTime(formats=["%Y-%m-%d"]),
              help='Harvest records not older than given date.')
@click.option('--fromdays', type=int, help='Harvest records not older than given days ago.')
@click.option('--clean', is_flag=True, help='Clean output folder before harvesting')
@click.option('--limit', type=int, help='Limit')
@click.option('--linkcheck/--no-linkcheck', default=False, is_flag=True, help='do not check if URLs resolve in validation')
@click.option('--no-update', is_flag=True, help='do not update existing record')
@click.option('--https', '-s', is_flag=True, help='enable upload on https')
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.pass_context
def combine(ctx, repo, iphost, auth, fromdate, fromdays, clean, limit, linkcheck, no_update, https, insecure):
    try:
        # harvest
        cmd = Harvest(
            repo=repo,
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
            repo=repo,
            outdir=ctx.obj['outdir'],)
        cmd.run(format='ckan', force=False, linkcheck=linkcheck, limit=limit,
                silent=ctx.obj['silent'])
        # upload
        upload = Upload(outdir=ctx.obj['outdir'], repo=repo)
        upload.run(iphost=iphost, auth=auth, target='ckan', from_=None, limit=limit,
                   no_update=no_update, verify=not insecure,
                   silent=ctx.obj['silent'], https=https)
    except UserInfo as e:
        click.echo(f'{e}')
    except Exception as e:
        logging.critical(f"combine: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--repo', '-c', required=False,
              help='delete all datasets of this repo')
@click.option('--dataset', '-d', metavar='DATASET_ID', required=False, help='delete single dataset')
@click.option('--iphost', '-i', required=True, help='IP address of CKAN instance')
@click.option('--auth', required=True, help='CKAN API key')
@click.option('--https', '-s', is_flag=True, help='enable purge on https')
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.pass_context
def purge(ctx, repo, dataset, iphost, auth, https, insecure):
    try:
        purge = Purge(repo=repo)
        purge.run(iphost=iphost, dataset=dataset, auth=auth, https=https, verify=not insecure, silent=ctx.obj['silent'])
    except Exception as e:
        logging.critical(f"purge: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


@cli.command()
@click.option('--repo', '-c', help='Repository')
@click.option('--iphost', '-i', required=True, help='IP address of CKAN instance', default="b2find.eudat.eu")
@click.option('--insecure', '-k', is_flag=True, help='Disable SSL verification')
@click.option('--limit', type=int, help='Limit of shown datasets', default=20)
@click.option('--pattern', help='Search criteria', default="")
@click.pass_context
def search(ctx, repo, iphost, insecure, limit, pattern):
    try:
        search = Search(repo=repo)
        search.run(iphost=iphost, limit=limit, pattern=pattern, verify=not insecure, silent=ctx.obj['silent'])
    except Exception as e:
        logging.critical(f"search: {e}", exc_info=True)
        raise click.ClickException(f"{e}")


if __name__ == '__main__':
    cli(obj={})
