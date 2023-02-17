from .base import Repository
from ..service_types import SchemaType, ServiceType


class DanseasyDatacite(Repository):
    IDENTIFIER = 'danseasy'
    URL = 'https://easy.dans.knaw.nl/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2020-10-01'
    REPOSITORY_ID = 're3data:r3d100010214'
    REPOSITORY_NAME = 'DANS-EASY'

    def update(self, doc):
        if not doc.doi:
            doc.doi = self.find_doi('alternateIdentifier')
        if not doc.source:
            doc.source = self.find_source('alternateIdentifier')
