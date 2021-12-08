from .base import Community
from ..service_types import SchemaType, ServiceType


class SciencesPoDDI25(Community):
    NAME = 'sciencespo'
    IDENTIFIER = NAME
    URL = 'https://data.sciencespo.fr/oai'
    SCHEMA = SchemaType.DDI25
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_ddi'
    OAI_SET = None
#   PRODUCTIVE = True
#   DATE = '2021-10-20'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Social Sciences')
