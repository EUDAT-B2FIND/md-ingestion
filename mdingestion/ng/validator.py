import colander
import pathlib
import json

from .writer import B2FWriter
from .core import B2FSchema
from .linkcheck import LinkChecker

import logging


class Validator(object):
    def __init__(self, linkcheck=True):
        # TODO: write also pandas csv file for statistical evaluation
        self.schema = B2FSchema()
        self.writer = B2FWriter()
        self.linkcheck = linkcheck
        self.lc = LinkChecker()
        self.summary = {
            'total': 0,
            'valid': 0,
            'written': 0,
            'broken_links': [],
            'required': {
                'title': 0,
                'identifier': 0,
                'publisher': 0,
                'publication_year': 0,
                'discipline': 0,
            },
            'optional': {},
            'invalid': {},
            'values': {},
            'invalid_values': {},
            'missing': [field.name for field in self.schema.children]
        }

    def validate(self, doc):
        # TODO: use counter? https://pymotw.com/2/collections/counter.html
        if self.linkcheck:
            self.lc.add(doc)
            self.summary['broken_links'] = self.lc.broken
        jsondoc = self.writer.json(doc)
        self.summary['total'] += 1
        try:
            self.schema.deserialize(jsondoc)
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

    def _update_summary(self, fields, valid=True):
        if valid:
            for key, value in fields.items():
                if not value:
                    continue
                if key in self.summary['missing']:
                    self.summary['missing'].remove(key)
                self._update_values(key, value, valid=valid)
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
                self._update_values(key, value, valid=valid)

    def _update_values(self, key, value, valid=True, max_value_length=250, max_values=25):
        if valid:
            values_key = 'values'
        else:
            values_key = 'invalid_values'
        if key not in self.summary[values_key]:
            self.summary[values_key][key] = {}
        # collect not more than "max_values" different values.
        if len(self.summary[values_key][key]) < max_values:
            val_key = str(value)[:max_value_length]
        # otherwiese just count the remaining values
        else:
            val_key = '[..]'
        # count values
        if val_key not in self.summary[values_key][key]:
            self.summary[values_key][key][val_key] = 1
        else:
            self.summary[values_key][key][val_key] += 1

    def print_summary(self):
        print("\nSummary:")
        print(f"\tvalid={self.summary['valid']}/{self.summary['total']}")
        print(f"\twritten={self.summary['written']}")
        print(f"\tbroken links={len(self.summary['broken_links'])}")
        print(f"\tinvalid geometry={len(self.summary['_errors_']['invalid_geometry'])}")
        print("\nRequired Fields:")
        for key, value in self.summary['required'].items():
            print(f"\t{key}={value}")
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
        if self.summary['missing']:
            print("\nMissing Fields:")
            for key in self.summary['missing']:
                print(f"\t{key}")

    def write_summary(self, outdir=None):
        outdir = outdir or pathlib.Path.cwd()
        out = outdir.joinpath('summary.json')
        # out.parent.mkdir(parents=True, exist_ok=True)
        with out.open(mode='w') as outfile:
            json.dump(self.summary, outfile, indent=4, sort_keys=True, ensure_ascii=False)
