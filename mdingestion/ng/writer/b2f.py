import os
import pathlib
import json

from .base import Writer

import logging


class B2FWriter(Writer):
    def write(self, doc, filename):
        data = self.json(doc)
        self.write_output(data, filename)

    def write_output(self, data, filename):
        source_path = pathlib.Path(filename)
        path_parts = list(source_path.parts)
        path_parts[-2] = 'b2f'
        path_parts[-1] = source_path.name.replace(source_path.suffix, '.json')
        out = pathlib.Path(*path_parts)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open(mode='w') as outfile:
            json.dump(data, outfile, indent=4, sort_keys=True, ensure_ascii=False)
            logging.info(f'map output written to {out}')

    def json(self, doc):
        data = {
            'identifier': doc.identifier,
            'title': doc.title,
            'description': doc.description,
            'tags': doc.tags,
            'doi': doc.doi,
            'pid': doc.pid,
            'source': doc.source,
            'related_identifier': doc.related_identifier,
            'metadata_access': doc.metadata_access,
            'creator': doc.creator,
            'contributor': doc.contributor,
            'publisher': doc.publisher,
            'publication_year': doc.publication_year,
            'rights': doc.rights,
            'open_access': doc.open_access,
            'contact': doc.contact,
            'language': doc.language,
            'resource_type': doc.resource_type,
            'format': doc.format,
            'discipline': doc.discipline,
            'spatial_coverage': doc.spatial_coverage,
            'spatial': doc.spatial,
            'temporal_coverage': doc.temporal_coverage,
            'temporal_coverage_begin_date': doc.temporal_coverage_begin_date,
            'temporal_coverage_end_date': doc.temporal_coverage_end_date,
            'temp_coverage_begin': doc.temp_coverage_begin,
            'temp_coverage_end': doc.temp_coverage_end,
            'oai_set': doc.oai_set,
            'oai_identifier': doc.oai_identifier,
        }
        return data
