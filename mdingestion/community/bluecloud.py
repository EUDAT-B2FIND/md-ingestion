import shapely
import json

from .base import Community
from ..service_types import SchemaType, ServiceType

from ..format import format_value


def fix_list(value):
    if value:
        if isinstance(value, list):
            fix = value
        else:
            fix = [value]
    else:
        fix = []
    return fix


class Bluecloud(Community):
    NAME = 'bluecloud'
    IDENTIFIER = NAME
    URL = 'https://data.blue-cloud.org/api/collections'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.BC
    PRODUCTIVE = False

    def _find(self, name):
        return fix_list(self.reader.parser.doc.get(name))

    def update(self, doc):
        doc.discipline = ['Marine Science']
        doc.description = self._find('Abstract')
        doc.source = self._find('OnlineResourceUrl')
        #doc.relatedIdentifier = self.find('linkAskeladden')
        doc.publication_year = self._find('Last_Update')
        doc.contributor = self._find('Organisations')
        #doc.language = ['Norwegian']
        #doc.contact = ['askeladden.hjelp@ra.no']
        #doc.creator = self.find('properties.opphav')
        #doc.rights = ['NLOD (https://data.norge.no/nlod/en/2.0/)']
        #doc.version = self.find('properties.versjonId')
        doc.title = self._find('Title') or self._find('Abstract')
        doc.keywords = self._find('Keywords')
        doc.temporal_coverage_begin_date = self._find('Temporal_Extent_Begin')
        doc.temporal_coverage_end_date = self._find('Temporal_Extent_End')
        doc.geometry = self.geometry()
        doc.instrument = self.instruments()
        doc.publisher = self.publishers()

    # TODO: fix list problem in json parser = json.py in Carsten weiss wo
    def instruments(self):
        instruments = []
        instruments.extend(self._find('Instruments'))
        instruments.extend(self._find('Platforms'))
        return instruments

    def publishers(self):
        publishers = ['Blue-Cloud']
        publishers.extend(self._find('Source'))
        return publishers

    def geometry(self):
        try:
            south = self.reader.parser.doc.get('Bounding_Box_SouthLatitude')
            west = self.reader.parser.doc.get('Bounding_Box_WestLongitude')
            north = self.reader.parser.doc.get('Bounding_Box_NorthLatitude')
            east = self.reader.parser.doc.get('Bounding_Box_EastLongitude')
            # bbox: minx=west, miny=south, maxx=east, maxy=north
            geometry = shapely.geometry.box(west, south, east, north)
        except Exception:
            geometry = None
        return geometry
