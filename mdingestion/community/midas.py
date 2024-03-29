from .base import Repository
from ..service_types import SchemaType, ServiceType


class MIDASDublinCore(Repository):
    IDENTIFIER = 'midas'
    URL = 'https://midas.lt/web/action/oaipmh'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True
    Date = '2021-07-05'
    REPOSITORY_ID = 're3data:r3d100012447'
    REPOSITORY_NAME = 'MIDAS - National Open Access Research Data Archive'

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'Lithuanian National Open Access Research Data Archive (MIDAS)'
