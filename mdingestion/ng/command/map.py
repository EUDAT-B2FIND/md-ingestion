from tqdm import tqdm

from .base import Command
from ..util import parse_source_list
from ..walker import Walker
from ..community import (
    EnvidatDatacite,
    EnvidatISO19139,
    ESSDatacite,
    DarusDatacite,
    SLKSDublinCore,
    Herbadrop,
)
from ..writer import CKANWriter

import logging


MAPPER = {
    'envidat-datacite': EnvidatDatacite,
    'envidat-oai_datacite': EnvidatDatacite,
    'envidat-iso19139': EnvidatISO19139,
    'ess-oai_datacite': ESSDatacite,
    'darus-datacite': DarusDatacite,
    'darus-oai_datacite': DarusDatacite,
    'slks-dc': SLKSDublinCore,
    'herbadrop-hjson': Herbadrop,
}


def mapper_factory(community, mdprefix):
    logging.debug(f'community={community}, mdprefix={mdprefix}')
    return MAPPER.get(f'{community}-{mdprefix}')


class Map(Command):
    def __init__(self, community, url=None, mdprefix=None, mdsubset=None, outdir=None, source_list=None):
        self.sources = parse_source_list(source_list)
        source = self.sources.get(community, dict())
        self.community = community
        self.url = url or source.get('url')
        self.mdprefix = mdprefix or source.get('mdprefix')
        mdsubset = mdsubset or source.get('mdsubset')
        self.mdsubset = mdsubset or 'SET_1'
        self.outdir = outdir
        self.walker = Walker(outdir)
        self.map_tool = mapper_factory(self.community, self.mdprefix)
        self.writer = CKANWriter()

    def run(self):
        for filename in tqdm(self.walk(), ascii=True, desc="Mapping", unit=' records'):
            self.map(filename)

    def walk(self):
        for filename in self.walker.walk_community(
                community=self.community,
                mdprefix=self.mdprefix,
                mdsubset=self.mdsubset,
                ext=self.map_tool.extension()):
            yield filename

    def map(self, filename):
        doc = self.map_tool(filename, self.url, self.community, self.mdprefix)
        logging.info(f'map: community={self.community}, mdprefix={self.mdprefix}, file={filename}')
        self.writer.write(doc, filename)
