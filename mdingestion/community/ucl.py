from .base import Repository
from ..service_types import SchemaType, ServiceType


class Ucl(Repository):
    IDENTIFIER = 'ucl'
    URL = 'https://api.figshare.com/v2/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'portal_549'
    PRODUCTIVE = True
    DATE = '2020-09-22'
    REPOSITORY_ID = 're3data:r3d100013090'
    REPOSITORY_NAME = 'UCL'

    def update(self, doc):
        doc.contributor = 'Figshare'
        doc.contact = 'researchdatarepository@ucl.ac.uk'
        doc.publisher = 'University College London UCL'
        doc.language = 'eng'
