#!/usr/bin/env python
import click
import requests
from tqdm import tqdm

from mdingestion.ng.harvester import harvester

import logging
logging.basicConfig(filename='update-deims.log', level=logging.INFO)

CSW_URL = 'https://deims.org/pycsw/catalogue/csw'
UPDATE_CACHE_URL = 'https://deims.org/data/iso19139/gmd/dataset'


def list_identifiers(limit=None):
    limit = limit or 1
    my_harvester = harvester(
        url=CSW_URL,
        mdprefix='dc',
        community='deims',
        verb='csw',
        fromdate=None,
        limit=limit,
        outdir=None,
        verify=True,
        mdsubset=None)
    for rec in my_harvester.harvest():
        yield rec.identifier


def update_iso_cache(identifier):
    data = {'uuid': identifier}
    resp = requests.get(UPDATE_CACHE_URL, params=data)
    return resp.ok


@click.command()
@click.option('--limit', type=int, help='Limit')
def update(limit):
    limit = limit or -1
    error_count = 0
    for identifier in tqdm(
        list_identifiers(limit=limit),
        ascii=True,
        desc="Updating",
        unit=' records',
        total=limit,
    ):
        ok = update_iso_cache(identifier)
        if not ok:
            logging.warning(f'Could not update record with identifier={identifier}')
            error_count += 1
    click.echo(f'errors {error_count}/{limit}')


if __name__ == '__main__':
    update()
