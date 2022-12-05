from .base import Repository
from ..service_types import SchemaType, ServiceType


class Bbmri(Repository):
    NAME = 'bbmri'
    IDENTIFIER = 'bbmri'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = '99916f6f-9a2c-4feb-a342-6552ac7f1529'  # BBMRI
    PRODUCTIVE = False

    def update(self, doc):
        doc.contributor = 'B2SHARE'
