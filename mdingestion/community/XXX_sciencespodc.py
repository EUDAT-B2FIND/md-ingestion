from .base import Repository
from ..service_types import SchemaType, ServiceType


# TODO: reingest, integration prepared on http://eudat7-ingest.dkrz.de/group/sciencespodc
class SciencesPoDublinCore(Repository):
    IDENTIFIER = 'sciencespodc'
    URL = 'https://data.sciencespo.fr/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
#   PRODUCTIVE = False
#   DATE = '2021-10-20'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Social Sciences')
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
