from .base import Repository
from ..service_types import SchemaType, ServiceType


class RodareDataCite(Repository):
    NAME = 'rodare'
    IDENTIFIER = NAME
    URL = 'https://rodare.hzdr.de/oai2d'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'user-rodare'
    PRODUCTIVE = True
    DATE = '2021-11-16'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Life Sciences; Natural Sciences; Engineering Sciences')
        doc.contact = 'https://rodare.hzdr.de/support'
