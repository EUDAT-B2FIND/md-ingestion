from .base import Repository
from ..service_types import SchemaType, ServiceType


class Edmond(Repository):
    NAME = 'edmond'
    TITLE = 'Edmond'
    IDENTIFIER = NAME
    URL = 'https://edmond.mpdl.mpg.de/api/search'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.Dataverse
    PRODUCTIVE = False
    DATE = '2023-02-16'
    REPOSITORY_ID = 'http://doi.org/10.17616/R3N33V'
    REPOSITORY_NAME = 'EDMOND'

    def update(self, doc):
        doc.doi = self.find_doi('global_id')
        doc.description = self.find('description')
        doc.source = self.find('url')
        # doc.relatedIdentifier = self.find('$..relatedDatasets.value')
        doc.publisher = self.find('publisher')
        doc.publication_year = self.find('published_at')
        doc.language = ['English']
        doc.contact = self.find('$..datasetContactEmail.value')
        doc.creator = self.find('authors')
        # doc.rights = ['NLOD (https://data.norge.no/nlod/en/2.0/)']
        # doc.places = self.find('properties.kommune')
        doc.version = self.find('majorVersion')
        doc.resource_type = 'Dataset'
        doc.funding_reference = self.funding(doc)
        doc.title = self.find('name')
        doc.keywords = self.find('keywords')
        # doc.geometry = self.geometry()

    def funding(self, doc):
        funds = self.find('$..grantNumberValue.value')
        funds.extend(self.find('$..grantNumberAgency.value'))
        return funds
