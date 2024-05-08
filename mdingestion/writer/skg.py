import os
import pathlib
import json

from .base import Writer, clean_fields

import logging


class SkgWriter(Writer):
    format = 'skg'

    def write(self, doc, filename):
        data = clean_fields(self.json(doc))
        self.write_output(data, filename)

    def write_output(self, data, filename):
        source_path = pathlib.Path(filename)
        path_parts = list(source_path.parts)
        path_parts[-2] = self.format
        path_parts[-1] = source_path.name.replace(source_path.suffix, '.json')
        out = pathlib.Path(*path_parts)
        out.parent.mkdir(parents=True, exist_ok=True)
        # TODO: fix outdir
        self.outdir = out.parent.absolute()
        with out.open(mode='w') as outfile:
            json.dump(data, outfile, indent=4, sort_keys=True, ensure_ascii=False)
            logging.info(f'map output written to {out}')

    def json(self, doc):
        data = {
            'repo': doc.repo,
            'groups': doc.groups,
            'identifier': doc.identifier,
            'title': doc.title,
            'description': doc.description,
            'keywords': doc.keywords,
            'doi': doc.doi,
            'pid': doc.pid,
            'source': doc.source,
            'related_identifier': doc.related_identifier,
            'metadata_access': doc.metadata_access,
            'creator': doc.creator,
            'contributor': doc.contributor,
            'instrument': doc.instrument,
            'publisher': doc.publisher,
            'publication_year': doc.publication_year,
            'funding_reference': doc.funding_reference,
            'rights': doc.rights,
            'open_access': doc.open_access,
            'contact': doc.contact,
            'language': doc.language,
            'resource_type': doc.resource_type,
            'format': doc.format,
            'size': doc.size,
            'version': doc.version,
            'discipline': doc.discipline,
            'accept': doc.accept,
            'spatial_coverage': doc.spatial_coverage,
            'spatial': doc.wkt,
            'temporal_coverage': doc.temporal_coverage,
            'temporal_coverage_begin_date': doc.temporal_coverage_begin_date,
            'temporal_coverage_end_date': doc.temporal_coverage_end_date,
            'oai_set': doc.oai_set,
            'oai_identifier': doc.oai_identifier,
        }
        return data