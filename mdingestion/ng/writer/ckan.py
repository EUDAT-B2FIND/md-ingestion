import os
import pathlib
import json

from .base import Writer

import logging


class CKANWriter(Writer):
    def write(self, doc, filename):
        data = self.json(doc)
        self.write_output(data, filename)

    def write_output(self, data, filename):
        source_path = pathlib.Path(filename)
        path_parts = list(source_path.parts)
        path_parts[-2] = 'json'
        path_parts[-1] = source_path.name.replace(source_path.suffix, '.json')
        out = pathlib.Path(*path_parts)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open(mode='w') as outfile:
            json.dump(data, outfile, indent=4, sort_keys=True, ensure_ascii=False)
            logging.info(f'map output written to {out}')

    def json(self, doc):
        data = self._ckan_fields(doc)
        data.update(self._extra_fields(doc))
        data.update(self._oai_fields(doc))
        return data

    def _extra_fields(self, doc):
        data = {
            'DOI': doc.doi,
            'RelatedIdentifier': doc.related_identifier,
            'MetaDataAccess': doc.metadata_access,
            'Contributor': doc.contributor,
            'Publisher': doc.publisher,
            'PublicationYear': doc.publication_year,
            'Rights': doc.rights,
            'OpenAccess': doc.open_access,
            'Contact': doc.contact,
            'Language': doc.language,
            'ResourceType': doc.resource_type,
            'Format': doc.format,
            'Discipline': doc.discipline,
            'DiscHierarchy': [],
            'SpatialCoverage': doc.spatial_coverage,
            'spatial': doc.spatial,
            'TemporalCoverage': doc.temporal_coverage,
            'TemporalCoverage:BeginDate': doc.temporal_coverage_begin_date,
            'TemporalCoverage:EndDate': doc.temporal_coverage_end_date,
            'TempCoverageBegin': doc.temp_coverage_begin,
            'TempCoverageEnd': doc.temp_coverage_end,
        }
        return data

    def _ckan_fields(self, doc):
        data = {}
        data['title'] = doc.title
        data['author'] = doc.creator
        data['notes'] = doc.description
        data['tags'] = doc.tags
        data['url'] = doc.source
        # data['version'] = ''  # checksum of doc
        data['owner_org'] = "eudat-b2find"
        data['name'] = doc.name
        data['group'] = doc.community
        data['groups'] = [{
            'name': doc.community,
        }]
        data['state'] = 'active'
        if doc.publication_year:
            data['PublicationTimestamp'] = f"{doc.publication_year[0]}-07-01T11:59:59Z"
        data['fulltext'] = doc.fulltext
        return data

    def _oai_fields(self, doc):
        data = {}
        if hasattr(doc, 'oai_set'):
            data['oai_set'] = doc.oai_set
            data['oai_identifier'] = doc.oai_identifier
        return data
