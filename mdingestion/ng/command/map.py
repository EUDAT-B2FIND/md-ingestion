from tqdm import tqdm
import colander
import pathlib
import json

from .base import Command
from ..walker import Walker
from ..community import reader
from ..writer import writer, B2FWriter
from ..core import B2FSchema

import logging


class Map(Command):
    """TODO: remove validation code from map"""
    def __init__(self, **args):
        super().__init__(**args)
        self.walker = Walker(self.outdir)
        self.reader = reader(self.community, self.mdprefix)
        self.writer = None
        # TODO: write also pandas csv file for statistical evaluation
        self.summary = {
            'total': 0,
            'valid': 0,
            'written': 0,
            'required': {
                'title': 0,
                'identifier': 0,
            },
            'optional': {},
            'invalid': {},
            'values': {},
        }

    def run(self, format=format, force=False, limit=None):
        limit = limit or -1
        # TODO: refactor writer init
        self.writer = writer(format)
        count = 0
        for filename in tqdm(self.walk(), ascii=True, desc=f"Map to {format}", unit=' records', total=limit):
            if limit > 0 and count >= limit:
                break
            doc = self.map(filename)
            is_valid = self.validate(doc)
            if force or is_valid:
                self.writer.write(doc, filename)
                self.summary['written'] += 1
            count += 1
        self.print_summary()
        self.write_summary()

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

    def validate(self, doc):
        # TODO: use counter? https://pymotw.com/2/collections/counter.html
        jsondoc = B2FWriter().json(doc)
        self.summary['total'] += 1
        try:
            B2FSchema().deserialize(jsondoc)
            self.summary['valid'] += 1
            valid = True
        except colander.Invalid as e:
            logging.warning(f"{e}")
            valid = False
            self._update_summary(e.asdict(), valid=False)
        except Exception as e:
            logging.warning(f"{e}")
            valid = False
        self._update_summary(jsondoc)
        return valid

    def _update_summary(self, fields, valid=True, max_value_length=100, max_values=100):
        if valid:
            for key, value in fields.items():
                if not value:
                    continue
                if key not in self.summary['values']:
                    self.summary['values'][key] = {}
                # collect not more than "max_values" different values.
                if len(self.summary['values'][key]) < max_values:
                    val_key = str(value)[:max_value_length]
                # otherwiese just count the remaining values
                else:
                    val_key = '[..]'
                # count values
                if val_key not in self.summary['values'][key]:
                    self.summary['values'][key][val_key] = 1
                else:
                    self.summary['values'][key][val_key] += 1
                # count required fields
                if key in self.summary['required']:
                    self.summary['required'][key] += 1
                # count optional fields
                elif key not in self.summary['optional']:
                    self.summary['optional'][key] = 1
                else:
                    self.summary['optional'][key] += 1
        else:
            # count invalid fields
            for key, value in fields.items():
                if key not in self.summary['invalid']:
                    self.summary['invalid'][key] = 1
                else:
                    self.summary['invalid'][key] += 1

    def print_summary(self):
        print("\nSummary:")
        print(f"\tvalid={self.summary['valid']}/{self.summary['total']}, written={self.summary['written']}")
        print("\nRequired Fields:")
        print(f"\ttitle={self.summary['required']['title']}, identifier={self.summary['required']['identifier']}")
        print("\nOptional Fields (complete):")
        for key, value in self.summary['optional'].items():
            if value == self.summary['total']:
                print(f"\t{key}")
        print("\nOptional Fields (incomplete):")
        for key, value in self.summary['optional'].items():
            if value < self.summary['total']:
                print(f"\t{key}={value}")
        if self.summary['invalid']:
            print("\nInvalid Fields:")
            for key, value in self.summary['invalid'].items():
                print(f"\t{key}={value}")

    def write_summary(self):
        out = pathlib.Path('summary.json')
        # out.parent.mkdir(parents=True, exist_ok=True)
        with out.open(mode='w') as outfile:
            json.dump(self.summary, outfile, indent=4, sort_keys=True, ensure_ascii=False)
