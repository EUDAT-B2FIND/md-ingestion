from .base import Community
from ..service_types import SchemaType, ServiceType


class SlksFF(Community):
    NAME = 'slks_ff'
    IDENTIFIER = 'slks_ff'
    URL = 'http://www.kulturarv.dk/ffrepox/OAIHandler'
    SCHEMA = SchemaType.FF
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'ff'
    OAI_SET = None

    def update(self, doc):
        doc.description = 'This record describes ancient sites and monuments as well as archaeological excavations undertaken by Danish museums.'
        doc.discipline = 'Archaeology'
        doc.publisher = 'Slots- og Kulturstyrelsen'
        doc.rights = 'For scientific use'
        doc.contact = 'post@slks.dk'
        doc.language = 'Danish'
        keywords = doc.keywords
        keywords.append('EOSC Nordic')
        keywords.append('Viking Age')
