from .base import Repository
from ..service_types import SchemaType, ServiceType


class DaticeDatacite(Repository):
    NAME = 'datice'
    IDENTIFIER = NAME
    URL = 'https://oai.datacite.org/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'GESIS.SSRI'
    PRODUCTIVE = True
    Date = '2021-08-20'

    def update(self, doc):
        doc.discipline = 'Social Sciences'
        doc.contact = 'gagnis@hi.is'
