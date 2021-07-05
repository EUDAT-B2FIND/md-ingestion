from .base import Community
from ..service_types import SchemaType, ServiceType


class MIDASDublinCore(Community):
    NAME = 'midas'
    IDENTIFIER = NAME
    URL = 'https://midas.lt/web/action/oaipmh'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True
    Date = '2021-07-05'

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'Lithuanian National Open Access Research Data Archive (MIDAS)'

