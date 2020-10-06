from .base import Community
from ..service_types import SchemaType, ServiceType


class GfzDatacite(Community):
    NAME = 'gfz'
    IDENTIFIER = NAME
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.GFZ'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geosciences')
        # doc.contact = im datacite reader auf contributor.ContactPerson mappen; wenn nicht vorhanden
