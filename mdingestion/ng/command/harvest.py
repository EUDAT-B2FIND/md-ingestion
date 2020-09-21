from tqdm import tqdm

from .base import Command
from ..harvester import harvester
from ..exceptions import UserInfo
from ..community import community

import logging


class Harvest(Command):

    def harvest(self, fromdate=None, limit=None, dry_run=False):
        _community = community(self.community)
        _harvester = harvester(
            community=_community.identifier,
            url=_community.url,
            service_type=_community.service_type,
            fromdate=fromdate,
            limit=limit,
            outdir=self.outdir,
            verify=self.verify)
        if dry_run:
            raise UserInfo(f'Found records={_harvester.total(limited=False)}')
        for record in tqdm(_harvester.harvest(),
                           ascii=True,
                           desc=f"Harvesting {self.community}",
                           unit=' records',
                           total=_harvester.total()):
            _harvester.write_record(record, pretty_print=True)
