from .base import Community
from ..service_types import SchemaType, ServiceType


class BaseNordicar(Community):
    NAME = 'nordicar'
    SCHEMA = SchemaType.?
    SERVICE_TYPE = ServiceType.?
    OAI_METADATA_PREFIX = ?
    PRODUCTIVE = False

    def update(self, doc):
        doc.keywords = self.keywords(doc)

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('Archaeology')
        return keywords


class Slks(BaseNordicar):
    NAME = 'slks'
    IDENTIFIER = NAME
    URL = 'https://www.archaeo.dk/ff/oai-pmh/'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True

class Askeladden(BaseNordicar):
    NAME = 'askeladden'
    IDENTIFIER = NAME
    URL = 'https://kart.ra.no/arcgis/rest/services/Distribusjon/Kulturminner20180301/MapServer/7/query'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.ArcGIS
    FILTER = "kulturminneKategori='Arkeologisk minne'"
    PRODUCTIVE = True
