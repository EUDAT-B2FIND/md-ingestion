from .base import Community
from ..service_types import SchemaType, ServiceType


class ESRFDatacite(Community):
    NAME = 'esrf'
    IDENTIFIER = 'esrf'
    URL = 'https://icatplus.esrf.fr/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None

    def update(self, doc):
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.publication_year =
        doc.temporal_coverage =
#        doc.keywords = self.keywords(doc)
        doc.publisher = 'ESRF (European Synchrotron Radiation Facility)'

#    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PaN')
        return keywords
