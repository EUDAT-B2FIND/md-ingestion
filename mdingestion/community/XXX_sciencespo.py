from .base import Repository
from ..service_types import SchemaType, ServiceType


# TODO: reingest, integration prepared on http://eudat7-ingest.dkrz.de/group/sciencespo
class SciencesPoDDI25(Repository):
    IDENTIFIER = 'sciencespo'
    URL = 'https://data.sciencespo.fr/oai'
    SCHEMA = SchemaType.DDI25
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_ddi'
    OAI_SET = None
#   PRODUCTIVE = True
#   DATE = '2021-10-20'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Social Sciences')
