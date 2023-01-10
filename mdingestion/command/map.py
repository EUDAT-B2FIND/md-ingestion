import os
from tqdm import tqdm

from .base import Command
from ..walker import Walker
from ..community import repo, repos
from ..writer import writer
from ..validator import Validator

import logging


class Map(Command):
    def __init__(self, **args):
        super().__init__(**args)
        self.walker = Walker(self.datadir)
        # self._repo = repos(self.repo)
        self.writer = None
        self.summary = {}

    def run(self, format=format, force=False, linkcheck=True, limit=None, silent=False):
        # TODO: refactor repo loop
        _repos = repos(self.repo)
        show = len(_repos) == 1 and not silent
        for identifier in tqdm(_repos,
                               ascii=True,
                               desc=f"Map {self.repo}",
                               # position=0,
                               unit=' repo',
                               total=len(_repos),
                               disable=len(_repos) == 1 or silent):
            self._repo = repo(identifier)
            self._run(format=format, force=force, linkcheck=linkcheck, limit=limit,
                      show=show, silent=silent)
        if len(_repos) > 1 and not silent:
            self.print_concise_summary()

    def print_concise_summary(self):
        print(f"\nMapping Summary for {self.repo}:")
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
        for filename in tqdm(self.walk(), ascii=True, desc=f"Map {self._repo.identifier} to {format}",
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
        validator.summary['_errors_'] = self._repo.errors
        validator.write_summary(prefix=self._repo.identifier, outdir=self.summary_dir, show=show)
        self.summary[self._repo.identifier] = validator.concise_summary()
        if not success:
            logging.warning(f"some files are not valid. repo={self._repo.identifier}")

    def walk(self):
        path = os.path.join(self._repo.identifier, 'raw')
        for filename in self.walker.walk(path=path, ext=self._repo.extension):
            yield filename

    def map(self, filename):
        doc = self._repo.read(filename)
        logging.info(f'map: repo={self._repo.identifier}, file={filename}')
        return doc
