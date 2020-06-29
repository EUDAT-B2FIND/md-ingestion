from tqdm import tqdm

from .base import Command
from ..walker import Walker
from ..community import reader
from ..writer import CKANWriter, B2FWriter
from ..core import B2FSchema

import logging


class Map(Command):
    def __init__(self, **args):
        super().__init__(**args)
        self.walker = Walker(self.outdir)
        self.reader = reader(self.community, self.mdprefix)
        self.writer = CKANWriter()
        self.summary = {
            'total': 0,
            'valid': 0,
            'required': {
                'title': 0,
                'source': 0,
            },
            'optional': {},
        }

    def run(self):
        for filename in tqdm(self.walk(), ascii=True, desc="Mapping", unit=' records'):
            doc = self.map(filename)
            self.update_summary(doc)
        self.print_summary()

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
        self.writer.write(doc, filename)
        return doc

    def update_summary(self, doc):
        # TODO: use counter? https://pymotw.com/2/collections/counter.html
        jsondoc = B2FWriter().json(doc)
        self.summary['total'] += 1
        try:
            B2FSchema().deserialize(jsondoc)
            self.summary['valid'] += 1
        except Exception:
            pass
        for key, value in jsondoc.items():
            if not value:
                continue
            if key in self.summary['required']:
                self.summary['required'][key] += 1
            elif key not in self.summary['optional']:
                self.summary['optional'][key] = 1
            else:
                self.summary['optional'][key] += 1

    def print_summary(self):
        print("\nSummary:")
        print(f"\tvalid={self.summary['valid']}/{self.summary['total']}")
        print("\nRequired Fields:")
        print(f"\ttitle={self.summary['required']['title']}, source={self.summary['required']['source']}")
        print("\nOptional Fields (complete):")
        for key, value in self.summary['optional'].items():
            if value == self.summary['total']:
                print(f"\t{key}")
        print("\nOptional Fields (incomplete):")
        for key, value in self.summary['optional'].items():
            if value < self.summary['total']:
                print(f"\t{key}={value}")
