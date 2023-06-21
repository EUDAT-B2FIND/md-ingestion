import json
import shapely
from .base import Repository
from ..service_types import SchemaType, ServiceType


class FIDmove(Repository):
    IDENTIFIER = 'fidmove'
    URL = 'https://data.fid-move.de/api/3/action/package_search?q&rows=1000000000000'
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

#    def update(self, doc):
        doc.discipline = ['Environmental Sciences']
        doc.contact = ['envidat@wsl.ch']
        doc.description = self.find('notes')
        doc.doi = self.find('doi')
        doc.source = self.find('url')
        doc.title = self.find('title')
        doc.resource_type = self.find('resource_type')
        doc.version = self.find('version')
        doc.places = self.find('spatial_info')
        doc.related_identifier = self.find('related_datasets')
        doc.language = self.find('language')
        doc.creator = self.creator(doc)
        doc.funding_reference = self.funding(doc)
        doc.rights = self.rights(doc)
        doc.keywords = self.keywords(doc)
        self.publisher(doc)
        self.tempcov(doc)
        self.geometry(doc)

    def keywords(self, doc):
        keys = []
        try:
            tags = self.reader.parser.doc.get('tags')
            for tag in tags:
                keys.append(tag['name'])
        except Exception:
            pass
        return keys

    def creator(self, doc):
        creas = []
        try:
            a = self.find('author')[0]
            authors = json.loads(a)
            for author in authors:
                crea = f"{author['given_name']}, {author['name']}, {author['identifier']}"
                creas.append(crea)
        except Exception:
            pass
        return creas

    def funding(self, doc):
        refs = []
        try:
            fun = self.find('funding')[0]
            funs = json.loads(fun)
            for funny in funs:
                ref = f"{funny['institution']}, {funny['grant_number']}"
                refs.append(ref)
        except Exception:
            pass
        return refs

    def rights(self,doc):
        r = []
        right = self.find('license_id')
        if right:
            r.extend(right)
        right = self.find('license_title')
        if right:
            r.extend(right)
        return r

    def publisher(self, doc):
        try:
            pub = self.find('publication')[0]
            doc.publisher = json.loads(pub)["publisher"]
            doc.publication_year = json.loads(pub)["publication_year"]
        except Exception:
            raise

    def tempcov(self, doc):
        try:
            temp = self.find('date')[0]
            temps = json.loads(temp)
            doc.temporal_coverage_begin_date = temps[0]["date"]
            doc.temporal_coverage_end_date = temps[0]["end_date"]
        except Exception:
            pass

    def geometry(self, doc):
        try:
            spatial = self.find('spatial')[0]
            geom = shapely.from_geojson(spatial)
            doc.geometry = geom
        except Exception:
            pass
