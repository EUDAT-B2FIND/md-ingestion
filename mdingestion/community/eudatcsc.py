from .base import Repository
from ..service_types import SchemaType, ServiceType


class EudatCSC(Repository):
    NAME = 'eudatcsc'
    IDENTIFIER = 'eudatcsc'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = 'e9b9792e-79fb-4b07-b6b4-b9c2bd06d095'  # EUDAT
    PRODUCTIVE = False

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'B2SHARE'
