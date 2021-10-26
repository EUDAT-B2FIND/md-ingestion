from tqdm import tqdm

from .base import Command
from ..harvester import harvester
from ..exceptions import UserInfo
from ..community import community, communities

import logging


class Harvest(Command):

    def harvest(self, fromdate=None, clean=False, limit=None, dry_run=False, silent=False,
                username=None, password=None):
        _communities = communities(self.community)
        for identifier in tqdm(_communities,
                               ascii=True,
                               desc=f"Harvesting {self.community}",
                               # position=0,
                               unit=' community',
                               total=len(_communities),
                               disable=len(_communities) == 1 or silent):
            try:
                self._harvest(identifier, fromdate=fromdate, clean=clean, limit=limit,
                              dry_run=dry_run, silent=silent, username=username, password=password)
            except Exception:
                msg = f"Harvesting of {identifier} failed."
                logging.exception(msg)
                raise Exception(msg)

    def _harvest(self, identifier, fromdate=None, clean=False, limit=None, dry_run=False, silent=False, 
                 username=None, password=None):
        _community = community(identifier)
        _harvester = harvester(
            community=_community.identifier,
            url=_community.url,
            service_type=_community.service_type,
            schema=_community.schema,
            oai_metadata_prefix=_community.oai_metadata_prefix,
            oai_set=_community.oai_set,
            filter=_community.filter,
            fromdate=fromdate,
            clean=clean,
            limit=limit,
            outdir=self.datadir,
            verify=self.verify,
            username=username,
            password=password)
        if dry_run:
            raise UserInfo(f'Found records={_harvester.total(limited=False)}')
        for record in tqdm(_harvester.harvest(),
                           ascii=True,
                           desc=f"Harvesting {identifier}",
                           unit=' records',
                           total=_harvester.total(),
                           disable=silent):
            _harvester.write_record(record, pretty_print=True)
