from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class DataverseNODatacite(Community):
    NAME = 'dataverseno'
    IDENTIFIER = 'dataverseno'
    URL = 'https://dataverse.no/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'dataverseno'
    PRODUCTIVE = True

    def update(self, doc):
        handle = format_value(self.find('resource.identifier', identifierType="Handle"), one=True)
        if handle:
            urls = self.reader.pid()
            urls.append(f'http://hdl.handle.net/{handle}')
            doc.pid = urls
        if not doc.publisher:
            doc.publisher = 'DataverseNO'

    # def keywords(self, doc):
        # keywords = doc.keywords
        # keywords.append('EOSC Nordic')
        # return keywords
