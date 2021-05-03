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
        self.walker = Walker(self.datadir)
        # self._community = community(self.community)
        self.writer = None
        self.summary = {}

    def run(self, format=format, force=False, linkcheck=True, limit=None, silent=False):
        # TODO: refactor community loop
        _communities = communities(self.community)
        show = len(_communities) == 1 and not silent
        for identifier in tqdm(_communities,
                               ascii=True,
                               desc=f"Map {self.community}",
                               # position=0,
                               unit=' community',
                               total=len(_communities),
                               disable=len(_communities) == 1 or silent):
            self._community = community(identifier)
            self._run(format=format, force=force, linkcheck=linkcheck, limit=limit,
                      show=show, silent=silent)
        if len(_communities) > 1 and not silent:
            self.print_concise_summary()

    def print_concise_summary(self):
        print(f"\nMapping Summary for {self.community}:")
        for name in self.summary.keys():
            summary = self.summary[name]
            print(f"\t{name}: {summary['valid']}/{summary['total']}")

    def _run(self, format=format, force=False, linkcheck=True, limit=None,
             show=True, silent=False):
        limit = limit or -1
        # TODO: refactor writer init
        self.writer = writer(format)
        # TODO: refactor validator usage
        validator = Validator(linkcheck=linkcheck)
        validator.summary['_invalid_files_'] = []
        count = 0
        success = True
        for filename in tqdm(self.walk(), ascii=True, desc=f"Map {self._community.identifier} to {format}",
                             unit=' records', total=limit, disable=silent):
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
                success = False
                validator.summary['_invalid_files_'].append(filename)
            count += 1
        validator.summary['_errors_'] = self._community.errors
        validator.write_summary(prefix=self._community.identifier, outdir=self.summary_dir, show=show)
        self.summary[self._community.identifier] = validator.concise_summary()
        if not success:
            logging.warning(f"some files are not valid. community={self._community.identifier}")

    def walk(self):
        path = os.path.join(self._community.identifier, 'raw')
        for filename in self.walker.walk(path=path, ext=self._community.extension):
            yield filename

    def map(self, filename):
        doc = self._community.read(filename)
        logging.info(f'map: community={self._community.identifier}, file={filename}')
        return doc
