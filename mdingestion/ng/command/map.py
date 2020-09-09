from tqdm import tqdm

from .base import Command
from ..walker import Walker
from ..community import reader
from ..writer import writer
from ..validator import Validator

import logging


class Map(Command):
    def __init__(self, **args):
        super().__init__(**args)
        self.walker = Walker(self.outdir)
        self.reader = reader(self.community, self.mdprefix)
        self.writer = None

    def run(self, format=format, force=False, linkcheck=True, limit=None):
        limit = limit or -1
        # TODO: refactor writer init
        self.writer = writer(format)
        # TODO: refactor validator usage
        validator = Validator(linkcheck=linkcheck)
        validator.summary['_invalid_files_'] = []
        count = 0
        for filename in tqdm(self.walk(), ascii=True, desc=f"Map to {format}", unit=' records', total=limit):
            if limit > 0 and count >= limit:
                break
            doc = self.map(filename)
            is_valid = validator.validate(doc)
            if force or is_valid:
                self.writer.write(doc, filename)
                validator.summary['written'] += 1
            else:
                logging.warning(f"validation failed: {filename}")
                validator.summary['_invalid_files_'].append(filename)
            count += 1
        validator.print_summary()
        validator.write_summary(self.writer.outdir)

    def walk(self):
        for filename in self.walker.walk_community(
                community=self.community,
                mdprefix=self.mdprefix,
                mdsubset=self.mdsubset,
                ext=self.reader.extension()):
            yield filename

    def map(self, filename):
        reader = self.reader()
        doc = reader.read(filename, self.url, self.community, self.mdprefix)
        logging.info(f'map: community={self.community}, mdprefix={self.mdprefix}, file={filename}')
        return doc
