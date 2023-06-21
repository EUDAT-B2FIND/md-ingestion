import json
import shapely
from .base import Repository
from ..service_types import SchemaType, ServiceType


class FIDmove(Repository):
    IDENTIFIER = 'fidmove'
    URL = 'https://data.fid-move.de/api/3'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.CKAN
    PRODUCTIVE = False
    DATE = ''
    REPOSITORY_ID = 're3data:r3d100014142'
    REPOSITORY_NAME = 'FID move'
    LOGO = ""
    LINK = 'https://data.fid-move.de/en/'
    DESCRIPTION = """The Research Data Repository of FID move is a long-term digital repository for open data in the field of transport and mobility research. All datasets are provided with an open licence and a persistent DataCite DOI (Digital Object Identifier). Both searching and archiving are free of charge.
The Specialised Information Service for Mobility and Transport Research (FID move) has been established by the Saxon State and University Library Dresden (SLUB) and the German National Library of Science and Technology (TIB – Leibniz Information Centre for Science and Technology) as part of the DFG funding programme "Specialised Information Services". The aim of FID move is the development and establishment of services and online tools in close consultation with the transport and mobility research community to support this community throughout the entire research cycle.
"""

    def update(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('notes')
        doc.doi = self.find_doi('doi')
#        doc.pid = self.pid()
        doc.source = self.find('url')
        doc.keywords = self._keywords(doc)
        doc.discipline = self.discipline(doc)
        doc.related_identifier = self.find('related_identifier')
        doc.creator = self.find('author')
        doc.publisher = self.find('maintainer')
#        doc.contributor = self.find('contributorName')
        doc.funding_reference = self.find('funding_reference')
        doc.publication_year = self.find('doi_date_published')
        doc.rights = self.rights(doc)
        doc.contact = self.find('author_email')
        doc.language = self.find('language')
        doc.resource_type = self.find('type')
        doc.format = self.format(doc)
        doc.size = self.size(doc)
        doc.version = self.find('version')
#        doc.temporal_coverage = self.find('date')
#        doc.geometry = self.find_geometry()
        doc.places = self.find('geolocation_place')

    def _keywords(self, doc):
        keys = []
        try:
            tags = self.reader.parser.doc.get('tags')
            for tag in tags:
                keys.append(tag['name'])
        except Exception:
            pass
        return keys

    def format(self, doc):
        formats = []
        try:
            resources = self.reader.parser.doc.get('resources')
            for resource in resources:
                formats.append(resource['mimetype'])
        except Exception:
            pass
        return formats

    def size(self, doc):
        sizes = []
        try:
            resources = self.reader.parser.doc.get('resources')
            for resource in resources:
                sizes.append(resource['size'])
        except Exception:
            pass
        return sizes

    def rights(self,doc):
        r = []
        right = self.find('license_id')
        if right:
            r.extend(right)
        right = self.find('license_title')
        if right:
            r.extend(right)
        return r
