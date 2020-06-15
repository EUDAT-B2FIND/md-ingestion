import requests
import json
import pathlib
import uuid
from tqdm import tqdm

from mdingestion.harvesting import Harvester as LegacyHarvester
from .base import Command
from ..util import parse_source_list

import logging


class Harvest(Command):
    def __init__(self, outdir=None, source_list=None):
        self.wrapped = LegacyHarvester(OUT=None, pstat=None, base_outdir=outdir, fromdate=None)
        self.sources = parse_source_list(source_list)
        self.outdir = outdir

    def harvest(self, community, url=None, verb=None, mdprefix=None, mdsubset=None, limit=None, verify=None):
        source = self.sources.get(community, dict())
        url = url or source.get('url')
        verb = verb or source.get('verb')
        mdprefix = mdprefix or source.get('mdprefix')
        mdsubset = mdsubset or source.get('mdsubset')
        mdsubset = mdsubset or 'SET_1'
        client = catalog_client_factory(verb, community, mdprefix, mdsubset, self.outdir)
        if client:
            for record in tqdm(client.fetch(url, limit=limit, verify=verify),
                               ascii=True, desc="Harvesting", unit=' records'):
                client.write_record(record)
        else:
            self.legacy_harvest(community, url, verb, mdprefix, mdsubset)

    def legacy_harvest(self, community, url, verb, mdprefix, mdsubset):
        request = [
            community,
            url,
            verb,
            mdprefix,
        ]
        self.wrapped.harvest(request)


def catalog_client_factory(verb, community, mdprefix, mdsubset, outdir):
    if verb == 'POST':  # 'herbadrop-api'
        return Herbadrop(community, mdprefix, mdsubset, outdir)
    return None


class CatalogClient(object):
    def __init__(self, community, mdprefix, mdsubset, outdir):
        self.community = community
        self.mdprefix = mdprefix
        self.mdsubset = mdsubset
        self.outdir = outdir
        self.format = 'xml'
        self.ext = 'xml'

    def identifier(self, record):
        pass

    def uid(self, record):
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, self.identifier(record)))

    def filename(self, record):
        out = pathlib.Path(
            self.outdir,
            f"{self.community}-{self.mdprefix}",
            self.mdsubset,
            self.format,
            f"{self.uid(record)}.{self.ext}")
        return out

    def fetch(self, url, limit, verify):
        pass

    def write_record(self, record):
        out = self.filename(record)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open(mode='w') as outfile:
            if self.ext == 'json':
                json.dump(record, outfile, indent=4, sort_keys=True, ensure_ascii=False)
            logging.debug(f'record written to {out}')


class Herbadrop(CatalogClient):
    def __init__(self, community, mdprefix, mdsubset, outdir):
        super().__init__(community, 'json', mdsubset, outdir)
        self.format = 'hjson'
        self.ext = 'json'

    def identifier(self, record):
        return record['depositIdentifier']

    def fetch(self, url, limit=10, verify=True):
        logging.captureWarnings(True)
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
        for record in records:
            yield record
