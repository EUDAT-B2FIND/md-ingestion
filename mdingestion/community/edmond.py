import shapely

from .base import Repository
from ..service_types import SchemaType, ServiceType


class Edmond(Repository):
    NAME = 'edmond'
    TITLE = 'Edmond'
    IDENTIFIER = NAME
    URL = 'https://edmond.mpdl.mpg.de'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.Dataverse
    PRODUCTIVE = True
    DATE = '2023-02-17'
    REPOSITORY_ID = 'http://doi.org/10.17616/R3N33V'
    REPOSITORY_NAME = 'EDMOND'

    def update(self, doc):
        doc.doi = self.find_doi('global_id')
        doc.description = self.find('description')
        doc.source = self.find('url')
        doc.publisher = self.find('publisher')
        doc.publication_year = self.find('published_at')
        doc.language = ['English']
        doc.contact = self.find('$..datasetContactEmail.value')
        doc.creator = self.find('authors')
        doc.title = self.find('name')
        doc.keywords = self.find('keywords')
        doc.rights = 'CC BY 4.0'
        doc.version = self.find('majorVersion')
        doc.resource_type = 'Dataset'
        doc.funding_reference = self.funding(doc)
        doc.related_identifier = self.rel(doc)
        doc.places = self.places(doc)
        doc.geometry = self.geo(doc)

    def funding(self, doc):
        funds = self.find('$..grantNumberValue.value')
        funds.extend(self.find('$..grantNumberAgency.value'))
        return funds

    def rel(self, doc):
        value = ''
        fields = self.reader.parser.doc['metadataBlocks']['citation']['fields']
        for field in fields:
            if field['typeName'] == 'relatedDatasets':
                value = field['value']
        return value

    def places(self, doc):
        vals = []
        fields = self.reader.parser.doc['metadataBlocks']['citation']['fields']
        for field in fields:
            if field['typeName'] == 'topicClassification':
                for value in field['value']:
                    if 'topicClassVocab' in value:
                        if value['topicClassVocab']['value'] == 'Geolocation – Place':
                            vals.append(value['topicClassValue']['value'])
        return vals

    def geo(self, doc):
        geometry = None
        fields = self.reader.parser.doc['metadataBlocks']['citation']['fields']
        for field in fields:
            if field['typeName'] == 'geolocation':
                for value in field['value']:
                    lon = None
                    lat = None
                    if 'geolocationLatitude' in value:
                        lat = value['geolocationLatitude']['value']
                        lat = float(lat)
                    if 'geolocationLongitude' in value:
                        lon = value['geolocationLongitude']['value']
                        lon = float(lon)
                    if lon and lat:
                        geometry = shapely.geometry.Point(lon, lat)
        return geometry
