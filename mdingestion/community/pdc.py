from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value, format_url


class Pdc(Community):
    NAME = 'pdc'
    IDENTIFIER = NAME
    URL = 'http://www.polardata.ca/oai/provider'
    SCHEMA = SchemaType.FGDC
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'fgdc'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
        if not doc.contributor:
            doc.contributor = 'Polar Data Catalogue'
        doc.discipline = self.discipline(doc, 'Environmental Research')
        doc.language = 'eng'
        # TODO: extracting DOI from <publish>
        # doc.doi = self.doi()

    def doi(self):
        url = ''
        txt = format_value(self.find('metadata.publish'), one=True)
        if 'doi' in txt:
            print(txt)
            value = txt.split('http://dx.doi.org/')[1]
            url = format_url(f"http://dx.doi.org/{value}")
        return url
