from tqdm import tqdm

from .base import Command
from ..harvester import harvester
from ..exceptions import UserInfo
from ..community import community, communities

import logging


class Harvest(Command):

    def harvest(self, fromdate=None, limit=None, dry_run=False):
        _communities = communities(self.community)
        for identifier in tqdm(_communities,
                               ascii=True,
                               desc=f"Harvesting {self.community}",
                               # position=0,
                               unit=' community',
                               total=len(_communities)):
            try:
                self._harvest(identifier, fromdate=fromdate, limit=limit, dry_run=dry_run)
            except Exception as e:
                logging.error(f"Harvesting off {identifier} failed.")

    def _harvest(self, identifier, fromdate=None, limit=None, dry_run=False):
        _community = community(identifier)
        _harvester = harvester(
            community=_community.identifier,
            url=_community.url,
            service_type=_community.service_type,
            schema=_community.schema,
            oai_metadata_prefix=_community.oai_metadata_prefix,
            oai_set=_community.oai_set,
            fromdate=fromdate,
            limit=limit,
            outdir=self.outdir,
            verify=self.verify)
        if dry_run:
            raise UserInfo(f'Found records={_harvester.total(limited=False)}')
        for record in tqdm(_harvester.harvest(),
                           ascii=True,
                           desc=f"Harvesting {identifier}",
                           unit=' records',
                           total=_harvester.total()):
            _harvester.write_record(record, pretty_print=True)
