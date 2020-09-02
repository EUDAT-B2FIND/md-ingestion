import os
import pathlib
import json
import hashlib

from .base import Writer, clean_fields

import logging


def map_ckan_fields(fields):
    ckan_fields = dict()
    for key, value in fields.items():
        if value:
            if key == 'author':
                if isinstance(value, list):
                    value = '; '.join(value)
            elif key in ['title', 'notes']:
                if isinstance(value, list):
                    value = '\n'.join(value)
            ckan_fields[key] = value
    return ckan_fields


def map_extra_fields(fields):
    extras = []
    for key, value in fields.items():
        if value:
            if isinstance(value, list):
                value = '; '.join(value)
            extras.append(dict(key=key, value=value))
    return extras


class CKANWriter(Writer):
    format = 'ckan'

    def write(self, doc, filename):
        data = self.json(doc)
        self.update_version(data)
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
        data = map_ckan_fields(self._ckan_fields(doc))
        data['extras'] = map_extra_fields(self._extra_fields(doc))
        data['extras'].extend(map_extra_fields(self._oai_fields(doc)))
        return data

    def update_version(self, data):
        checksum = hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()).hexdigest()
        data['version'] = checksum

    def _extra_fields(self, doc):
        data = {
            'Creator': doc.creator,
            'DOI': doc.doi,
            'PID': doc.pid,
            'RelatedIdentifier': doc.related_identifier,
            'MetaDataAccess': doc.metadata_access,
            'Contributor': doc.contributor,
            'Instrument': doc.instrument,
            'Publisher': doc.publisher,
            'PublicationYear': doc.publication_year,
            'FundingReference': doc.funding_reference,
            'Rights': doc.rights,
            'OpenAccess': 'true' if doc.open_access else 'false',
            'Contact': doc.contact,
            'Language': doc.language,
            'ResourceType': doc.resource_type,
            'Format': doc.format,
            'Size': doc.size,
            'Version': doc.version,
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
        data['tags'] = [dict(name=tag) for tag in doc.keywords]
        data['url'] = doc.source
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
