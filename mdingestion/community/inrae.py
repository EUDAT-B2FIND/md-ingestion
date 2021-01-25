from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class DataverseNODatacite(Community):
    NAME = 'inrae'
    IDENTIFIER = 'inrae'
    URL = 'https://data.inrae.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'NoGeneticResource'

    def update(self, doc):
        handle = format_value(self.find('resource.identifier', identifierType="Handle"), one=True)
        if handle:
            urls = self.reader.pid()
            urls.append(f'http://hdl.handle.net/{handle}')
            doc.pid = urls
        if not doc.publisher:
            doc.publisher = 'INRAe'

    # def keywords(self, doc):
        # keywords = doc.keywords
        # keywords.append('EOSC Nordic')
        # return keywords
