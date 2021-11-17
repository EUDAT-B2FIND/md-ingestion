from .base import Community
from ..service_types import SchemaType, ServiceType


# class B2ShareCSC(Community):
#   NAME = 'b2share_csc'
#   IDENTIFIER = 'b2share_csc'
#   URL = 'https://b2share.eudat.eu/api/oai2d'
#   SCHEMA = SchemaType.Eudatcore
#   SERVICE_TYPE = ServiceType.OAI
#   OAI_METADATA_PREFIX = 'eudatcore'
#   OAI_SET = 'e9b9792e-79fb-4b07-b6b4-b9c2bd06d095'  # EUDAT
#   PRODUCTIVE = False

#   def update(self, doc):
#       super().update(doc)
#           doc.keywords = self.keywords(doc, 'whatever')
#           if not doc.publisher:
#           doc.publisher = 'B2SHARE'
