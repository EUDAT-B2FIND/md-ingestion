from .base import Repository
from ..service_types import SchemaType, ServiceType


class DaticeDatacite(Repository):
    IDENTIFIER = 'datice'
    TITLE = 'DATICE'
    URL = 'https://oai.datacite.org/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'GESIS.SSRI'
    PRODUCTIVE = True
    DATE = '2021-08-20'
    REPOSITORY_ID = 're3data:r3d100013494'
    REPOSITORY_NAME = 'DATICE'

    def update(self, doc):
        doc.discipline = 'Social Sciences'
        doc.contact = 'gagnis@hi.is'
