from .panosc import BasePanosc
from ..service_types import SchemaType, ServiceType


class CERICDatacite(BasePanosc):
    IDENTIFIER = 'ceric'
    TITLE = 'CERIC'
    URL = 'https://data.ceric-eric.eu/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None

    PRODUCTIVE = True
    DATE = '2022-07-19'
    DESCRIPTION = "CERIC is a European Research Infrastructure Consortium (ERIC) integrating and providing open access to some of the best facilities in Europe, to help science and industry advance in all fields of materials, biomaterials and nanotechnology. With a single entry point to some of the leading national research infrastructures in 8 European countries, it enables the delivery of innovative solutions to societal challenges in the fields of energy, health, food, cultural heritage and more."
    LOGO = ''

    def update(self, doc):
        doc.source = self.source(doc)
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.keywords = self.keywords(doc)
        doc.temporal_coverage = self.find('dates.date', dateType="Collected")

    def source(self,doc):
        source = doc.source
        if not doc.identifier:
            source = self.find_source('alternateIdentifiers.alternateIdentifier')
        return source

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PaN')
        return keywords
    