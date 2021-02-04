from .base import Community
from ..service_types import SchemaType, ServiceType

from ..format import format_value


class Wikinger(Community):
    NAME = 'wikinger'
    IDENTIFIER = NAME
    URL = 'https://kart.ra.no/arcgis/rest/services/Distribusjon/Kulturminner20180301/MapServer/7/query'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.ArcGIS

    def update(self, doc):
        doc.title = self.find('properties.navn')
        doc.source = "http://wikinger.no"
        doc.publisher = "wikinger"
        doc.publication_year = "2021"
        doc.keywords = "wikinger"
