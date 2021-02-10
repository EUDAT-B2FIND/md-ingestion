from .base import Community
from ..service_types import SchemaType, ServiceType

from ..format import format_value


class Askeladden(Community):
    NAME = 'askeladden'
    IDENTIFIER = NAME
    URL = 'https://kart.ra.no/arcgis/rest/services/Distribusjon/Kulturminner20180301/MapServer/7/query'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.ArcGIS

    def update(self, doc):
        doc.title = self.find('properties.navn')
        doc.discipline = ['Archaeology']
        doc.description = self.find('properties.informasjon')
        doc.source = self.find('properties.linkKulturminnesok')
        doc.publisher = ['Askeladden']
        doc.publication_year = self.find('properties.forsteDigitaliseringsdato')
        doc.keywords = [self.find('properties.kulturminneKategori')[0], self.find('properties.kulturminneLokalitetArt')[0]]
        doc.places = self.find('properties.kommune')
        doc.version = self.find('properties.versjonId')

