from tqdm import tqdm

from .base import Command
from ..harvester import harvester
from ..exceptions import UserInfo

import logging


class Harvest(Command):

    def harvest(self, fromdate=None, limit=None, dry_run=False):
        _harvester = harvester(
            community=self.community,
            url=self.url,
            verb=self.verb,
            mdprefix=self.mdprefix,
            mdsubset=self.mdsubset,
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
