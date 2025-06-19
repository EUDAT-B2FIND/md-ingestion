from tqdm import tqdm

from .base import Command
from ..harvester import harvester
from ..exceptions import UserInfo
from ..community import repo, repos

import logging


class Harvest(Command):

    def harvest(self, fromdate=None, clean=False, limit=None, dry_run=False, silent=False,
                username=None, password=None):
        _repos = repos(self.repo)
        for identifier in tqdm(_repos,
                               ascii=True,
                               desc=f"Harvesting {self.repo}",
                               # position=0,
                               unit=' repo',
                               total=len(_repos),
                               disable=len(_repos) == 1 or silent):
            try:
                self._harvest(identifier, fromdate=fromdate, clean=clean, limit=limit,
                              dry_run=dry_run, silent=silent, username=username, password=password)
            except Exception:
                msg = f"Harvesting of {identifier} failed."
                logging.exception(msg)
                raise Exception(msg)

    def _harvest(self, identifier, fromdate=None, clean=False, limit=None, dry_run=False, silent=False,
                 username=None, password=None):
        _repo = repo(identifier)
        _harvester = harvester(
            repo=_repo.identifier,
            url=_repo.url,
            service_type=_repo.service_type,
            schema=_repo.schema,
            oai_metadata_prefix=_repo.oai_metadata_prefix,
            oai_set=_repo.oai_set,
            filter=_repo.filter,
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
            try:
                _harvester.write_record(record, pretty_print=True)
            except Exception:
                logging.exception(f"Harvesting of {identifier} failed.")
