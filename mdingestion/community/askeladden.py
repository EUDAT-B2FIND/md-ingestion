from shapely.geometry import shape

from .nordicar import BaseNordicar
from ..service_types import SchemaType, ServiceType


class Askeladden(BaseNordicar):
    IDENTIFIER = 'askeladden'
    TITLE = 'Askeladden'
    URL = 'https://kart.ra.no/arcgis/rest/services/Distribusjon/Kulturminner20180301/MapServer/7/query'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.ArcGIS
    FILTER = "kulturminneKategori='Arkeologisk minne'"
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = ['Archaeology']
        doc.description = self.find('properties.informasjon')
        doc.source = self.find('properties.linkKulturminnesok')
        doc.relatedIdentifier = self.find('linkAskeladden')
        doc.publisher = ['Askeladden']
        doc.publication_year = self.find('properties.forsteDigitaliseringsdato')
        doc.language = ['Norwegian']
        doc.contact = ['askeladden.hjelp@ra.no']
        doc.creator = self.find('properties.opphav')
        doc.rights = ['NLOD (https://data.norge.no/nlod/en/2.0/)']
        doc.places = self.find('properties.kommune')
        doc.version = self.find('properties.versjonId')
        doc.title = self.title()
        doc.keywords = self.keywords()
        doc.keywords = self.keywords_append(doc)
        doc.geometry = self.geometry()

    def title(self):
        title = self.find('properties.navn')
        if not title:
            title = 'Uten navn'
        elif len(title[0]) < 4:
            title = 'Uten navn'

        return title

    def keywords(self):
        keywords = []
        keyword = self.find('properties.kulturminneOpprinneligfunksjon')
        if keyword:
            keywords.append(keyword[0])
        keyword = self.find('properties.kulturminneKategori')
        if keyword:
            keywords.append(keyword[0])
        keyword = self.find('properties.kulturminneLokalitetArt')
        if keyword:
            keywords.append(keyword[0])
        return keywords

    def geometry(self):
        geom = shape(self.reader.parser.doc['geometry'])
        # return geom.centroid
        return geom
