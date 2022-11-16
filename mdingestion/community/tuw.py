from .base import Community
from ..service_types import SchemaType, ServiceType


class TuwDatacite(Community):
    NAME = 'tuw'
    IDENTIFIER = NAME
    URL = 'https://researchdata.tuwien.ac.at/oai2d'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 
    PRODUCTIVE = False
    Date = 

#    def update(self, doc):
#        doc.discipline = 'Social Sciences'
#        doc.contact = 'gagnis@hi.is'