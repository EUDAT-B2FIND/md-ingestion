from .base import Repository
from ..service_types import SchemaType, ServiceType


class Edmond(Repository):
    NAME = 'edmond'
    TITLE = 'Edmond'
    IDENTIFIER = NAME
    URL = 'https://edmond.mpdl.mpg.de'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.Dataverse
    # FILTER = ""
    PRODUCTIVE = False

    def update(self, doc):
        # doc.discipline = ['Edmond']
        doc.doi = self.find_doi('global_id')
        doc.description = self.find('description')
        doc.source = self.find('url')
        # doc.relatedIdentifier = self.find('linkAskeladden')
        doc.publisher = self.find('publisher')
        doc.publication_year = self.find('published_at')
        doc.language = ['English']
        doc.contact = self.find('contacts.name')
        doc.creator = self.find('authors')
        # print('creator', doc.creator)
        # doc.rights = ['NLOD (https://data.norge.no/nlod/en/2.0/)']
        # doc.places = self.find('properties.kommune')
        doc.version = self.find('majorVersion')
        doc.resource_type = 'Dataset'
        # doc.open_access = True
        doc.title = self.find('name')
        doc.keywords = self.find('keywords')
        # print('keys', doc.keywords)
        # append 'subject'
        # doc.keywords = self.keywords_append(doc)
        # doc.geometry = self.geometry()
