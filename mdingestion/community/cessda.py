from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..core.doc import OriginalProv


class CessdaDDI25(Repository):
    IDENTIFIER = 'cessda'
    URL = 'https://datacatalogue.cessda.eu/oai-pmh/v0/oai'
    SCHEMA = SchemaType.DDI25
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_ddi25'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2022-10-12'
    REPOSITORY_ID = 're3data:r3d100010202'
    REPOSITORY_NAME = 'CESSDA'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Social Sciences')
        doc.publisher = self.publisher()
        doc.origin_prov = self.origin_prov()

    def publisher(self):
        publisher = []
        publisher.extend(self.find('distrbtr'))
        if not publisher:
            publisher.append('CESSDA')
        return publisher

    def origin_prov(self):
        about = OriginalProv()
        about.harvest_date = '2023-11-06'
        about.altered = 'false'
        about.base_url = 'https://data.aussda.at/oai'
        about.identifier = ''
        about.datestamp = ''
        about.metadata_namespace = ''
        about.repository_id = ''
        about.repository_name = ''
        return about.as_string()