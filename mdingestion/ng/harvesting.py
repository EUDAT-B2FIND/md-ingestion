import requests
import json

from mdingestion.harvesting import Harvester as LegacyHarvester
from .command import Command
from .util import parse_source_list

import logging


class Harvester(Command):
    def __init__(self, outdir=None, source_list=None):
        self.wrapped = LegacyHarvester(OUT=None, pstat=None, base_outdir=outdir, fromdate=None)
        self.sources = parse_source_list(source_list)

    def harvest(self, community, url=None, verb=None, mdprefix=None):
        source = self.sources.get(community, dict())
        url = url or source.get('url')
        verb = verb or source.get('verb')
        mdprefix = mdprefix or source.get('mdprefix')
        client = catalog_client_factory(verb)
        if client:
            records = client.fetch(url, limit=1)
            print(records)
        else:
            self.legacy_harvest(community, url, verb, mdprefix)

    def legacy_harvest(self, community, url, verb, mdprefix):
        request = [
            community,
            url,
            verb,
            mdprefix,
        ]
        print(request)
        self.wrapped.harvest(request)


def catalog_client_factory(verb):
    if verb == 'herbadrop-api':  # 'POST':
        return Herbadrop()
    return None


class CatalogClient(object):
    def fetch(self, url, limit, verify):
        pass


class Herbadrop(CatalogClient):
    def fetch(self, url, limit=10, verify=False):
        data = {
            "text": "Herbarium",
            "searchTextInMetadata": True,
            "searchTextInAdditionalData": True,
            "page": 1,
            "size": limit
        }
        headers = {'content-type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(data), verify=verify)
        records = response.json()['result']
        return records
