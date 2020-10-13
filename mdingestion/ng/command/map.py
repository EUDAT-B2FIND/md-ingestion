import os
from tqdm import tqdm

from .base import Command
from ..walker import Walker
from ..community import community, communities
from ..writer import writer
from ..validator import Validator

import logging


class Map(Command):
    def __init__(self, **args):
        super().__init__(**args)
        self.walker = Walker(self.outdir)
        # self._community = community(self.community)
        self.writer = None

    def run(self, format=format, force=False, linkcheck=True, limit=None, summary_dir=None):
        _communities = communities(self.community)
        for identifier in tqdm(_communities,
                               ascii=True,
                               desc=f"Map {self.community}",
                               # position=0,
                               unit=' community',
                               total=len(_communities)):
            self._community = community(identifier)
            self._run(format=format, force=force, linkcheck=linkcheck, limit=limit, summary_dir=summary_dir)

    def _run(self, format=format, force=False, linkcheck=True, limit=None, summary_dir=None):
        limit = limit or -1
        # TODO: refactor writer init
        self.writer = writer(format)
        # TODO: refactor validator usage
        validator = Validator(linkcheck=linkcheck)
        validator.summary['_invalid_files_'] = []
        count = 0
        for filename in tqdm(self.walk(), ascii=True, desc=f"Map {self._community.identifier} to {format}",
                             unit=' records', total=limit):
            if limit > 0 and count >= limit:
                break
            logging.info(f'mapping {filename}')
            doc = self.map(filename)
            is_valid = validator.validate(doc)
            if force or is_valid:
                self.writer.write(doc, filename)
                validator.summary['written'] += 1
            else:
                logging.warning(f"validation failed: {filename}")
                validator.summary['_invalid_files_'].append(filename)
            count += 1
        validator.summary['_errors_'] = self._community.errors
        validator.write_summary(prefix=self.community, outdir=summary_dir)

    def walk(self):
        path = os.path.join(self._community.identifier, 'raw')
        for filename in self.walker.walk(path=path, ext=self._community.extension):
            yield filename

    def map(self, filename):
        doc = self._community.read(filename)
        logging.info(f'map: community={self.community}, file={filename}')
        return doc
