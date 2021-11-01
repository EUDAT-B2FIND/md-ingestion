import shapely
import json

from .base import Community
from ..service_types import SchemaType, ServiceType

from ..format import format_value


class Bluecloud(Community):
    NAME = 'bluecloud'
    IDENTIFIER = NAME
    URL = 'https://data.blue-cloud.org/api/collections'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.BC
    PRODUCTIVE = False

    def update(self, doc):
        doc.discipline = ['Marine Science']
        doc.description = self.find('Abstract')
        doc.source = self.find('OnlineResourceUrl')
        #doc.relatedIdentifier = self.find('linkAskeladden')
        doc.publication_year = self.find('Last_Update')
        doc.contributor = self.find('Organisations')
        #doc.language = ['Norwegian']
        #doc.contact = ['askeladden.hjelp@ra.no']
        #doc.creator = self.find('properties.opphav')
        #doc.rights = ['NLOD (https://data.norge.no/nlod/en/2.0/)']
        #doc.version = self.find('properties.versjonId')
        doc.title = self.find('Title') or self.find('Abstract')
        doc.keywords = self.find('Keywords')
        doc.temporal_coverage_begin_date = self.find('Temporal_Extent_Begin')
        doc.temporal_coverage_end_date = self.find('Temporal_Extent_End')
        doc.geometry = self.geometry()
        doc.instrument = self.instruments()
        doc.publisher = self.publishers()

    # TODO: fix list problem in json parser = json.py in Carsten weiss wo
    def instruments(self):
        instruments = []
        instruments.extend(self.reader.parser.doc['Instruments'])
        instruments.extend(self.reader.parser.doc['Platforms'])
        return instruments

    def publishers(self):
        publishers = ['Blue-Cloud']
        publishers.extend(self.find('Source'))
        return publishers

    def geometry(self):
        try:
            south = self.reader.parser.doc['Bounding_Box_SouthLatitude']
            west = self.reader.parser.doc['Bounding_Box_WestLongitude']
            north = self.reader.parser.doc['Bounding_Box_NorthLatitude']
            east = self.reader.parser.doc['Bounding_Box_EastLongitude']
            # bbox: minx=west, miny=south, maxx=east, maxy=north
            geometry = shapely.geometry.box(west, south, east, north)
        except Exception:
            geometry = None
        return geometry
