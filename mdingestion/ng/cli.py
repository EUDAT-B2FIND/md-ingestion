import click
import pathlib

from .harvesting import Harvester
from .mapping import Mapper
from .uploading import Uploader

import logging


CONTEXT_OBJ = dict()
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'], obj=CONTEXT_OBJ)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option('--debug/--no-debug', default=False)
@click.option('--list', '-l', default='ingestion_list', help='Filename with list of source.')
@click.option('--fromdate', help='Filter by date.')
@click.option('--outdir', '-o', default='oaidata',
              help='The absolute root dir in which all harvested files will be saved.')
@click.pass_context
def cli(ctx, debug, list, fromdate, outdir):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj['list'] = list
    ctx.obj['fromdate'] = fromdate

    out = pathlib.Path(outdir)
    if not out.is_absolute():
        out = pathlib.Path.cwd().joinpath(out)
    ctx.obj['outdir'] = out.absolute().as_posix()

    if debug:
        logging.basicConfig(filename='out.log', level=logging.DEBUG)
    else:
        logging.basicConfig(filename='out.log', level=logging.INFO)


@cli.command()
@click.option('--community', '-c', help='Community')
@click.option('--url', help='Source URL')
@click.option('--verb',
              help='Requests defined in OAI-PMH: ListRecords (default) or ListIdentifers.')
@click.option('--mdprefix', help='Metadata prefix')
@click.pass_context
def harvest(ctx, community, url, verb, mdprefix):
    harvester = Harvester(outdir=ctx.obj['outdir'], source_list=ctx.obj['list'])
    harvester.harvest(
        community=community,
        mdprefix=mdprefix,
        url=url,
        verb=verb,
    )


@cli.command()
@click.option('--community', '-c', help='Community')
@click.option('--mdprefix', help='Metadata prefix')
@click.pass_context
def map(ctx, community, mdprefix):
    mapper = Mapper(outdir=ctx.obj['outdir'], source_list=ctx.obj['list'],
                    community=community, mdprefix=mdprefix)
    with click.progressbar(list(mapper.walk())) as bar:
        for filename in bar:
            mapper.map(filename)
            # import time
            # time.sleep(1)


@cli.command()
@click.option('--community', '-c', help='Community')
@click.option('--iphost', help='IP address of CKAN instance')
@click.option('--auth', help='CKAN API key')
@click.pass_context
def upload(ctx, community, iphost, auth):
    uploader = Uploader(outdir=ctx.obj['outdir'], iphost=iphost, auth=auth)
    uploader.upload(community)


if __name__ == '__main__':
    cli(obj={})
