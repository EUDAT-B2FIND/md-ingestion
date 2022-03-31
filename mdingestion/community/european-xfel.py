from .base import Community
from ..service_types import SchemaType, ServiceType


class EuropeanXFEL(Community):
    NAME = 'european-xfel'
    IDENTIFIER = NAME
    URL = 'https://in.xfel.eu/metadata/oai-pmh/oai2'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = ['Natural Sciences','Life Sciences', 'Materials Science']
