from .base import Community
from ..service_types import SchemaType, ServiceType


class Edmond(Community):
    NAME = 'edmond'
    TITLE = 'Edmond'
    IDENTIFIER = NAME
    URL = 'https://demo.dataverse.org/api/search'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.Dataverse
    # FILTER = ""
    PRODUCTIVE = False

    def update(self, doc):
        # doc.discipline = ['Edmond']
        doc.description = self.find('description')
        doc.source = self.find('url')
        # doc.relatedIdentifier = self.find('linkAskeladden')
        doc.publisher = self.find('publisher')
        doc.publication_year = self.find('published_at')
        doc.language = ['English']
        doc.contact = self.find('contacts.name')
        doc.creator = self.find('authors')
        # doc.rights = ['NLOD (https://data.norge.no/nlod/en/2.0/)']
        # doc.places = self.find('properties.kommune')
        doc.version = self.find('versionId')
        doc.title = self.find('name')
        doc.keywords = self.find('keywords')
        # doc.keywords = self.keywords_append(doc)
        # doc.geometry = self.geometry()

    