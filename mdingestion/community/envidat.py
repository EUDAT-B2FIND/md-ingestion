import json
import shapely
from .base import Repository
from ..service_types import SchemaType, ServiceType


class Envidat(Repository):
    IDENTIFIER = 'envidat'
    TITLE = 'EnviDat'
    URL = 'https://www.envidat.ch/api'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.CKAN
    PRODUCTIVE = True
    DATE = '2023-06-14'
    REPOSITORY_ID = 're3data:r3d100012587'
    REPOSITORY_NAME = 'EnviDat'
    LOGO = ""
    LINK = 'https://www.envidat.ch'
    DESCRIPTION = """EnviDat is the environmental data portal developed by the Swiss Federal Research Institute WSL. EnviDat has the capability to integrate, host and publish environmental data sets. It provides efficient, unified and managed access for WSL's rich reservoir of environmental monitoring and research data, according to the data policy of WSL. Through the sharing of the unique and high-value data sets worldwide, EnviDat fosters research and contributes to advancing enviromental science."""

    def update(self, doc):
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
            doc.publisher = json.loads(pub).get("publisher", "EnviDat")
            if not doc.publisher:
                doc.publisher = "EnviDat"
            doc.publication_year = json.loads(pub).get("publication_year")
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
