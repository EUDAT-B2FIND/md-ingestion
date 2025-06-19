import shapely
from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..util import convert_to_lon_180


def fix_list(value):
    if value:
        if isinstance(value, list):
            fix = value
        elif isinstance(value, dict):
            fix = list(value.values())
        else:
            fix = [value]
    else:
        fix = []
    return fix


class Bluecloud(Repository):
    IDENTIFIER = 'bluecloud'
    URL = 'https://data.blue-cloud.org/api/collections'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.BC
    PRODUCTIVE = True
    DATE = '2021-04-27'

    def _find(self, name):
        return fix_list(self.reader.parser.doc.get(name))

    def update(self, doc):
        doc.discipline = ['Marine Science']
        doc.description = self._find('Abstract')
        doc.source = self.source()
        doc.publication_year = self._find('Last_Update')
        doc.contributor = self._find('Organisations')
        doc.contact = ['blue-cloud-support@maris.nl']
        doc.title = self._find('Title') or self._find('Abstract')
        doc.temporal_coverage_begin_date = self._find('Temporal_Extent_Begin')
        doc.temporal_coverage_end_date = self._find('Temporal_Extent_End')
        doc.geometry = self.geometry()
        doc.instrument = self.instruments()
        doc.keywords = self.keywords()
        doc.publisher = self.publishers()

    def source(self):
        source = []
        oru_urls = self._find("OnlineResourceUrl")
        for oru in oru_urls:
            if "http" in oru:
                url = oru
            else:
                url = f"https://data.blue-cloud.org{oru}"
            source.append(url)
        return source

    def instruments(self):
        instruments = []
        instruments.extend(self._find('Instruments'))
        instruments.extend(self._find('Platforms'))
        return instruments

    def keywords(self):
        keywords = []
        keywords.extend(self._find('Keywords'))
        keywords.extend(self._find('Parameters'))
        return keywords

    def publishers(self):
        publishers = ['Blue-Cloud Data Discovery & Access service']
        publishers.extend(self._find('Source'))
        return publishers

    def geometry(self):
        try:
            south = self.reader.parser.doc.get('Bounding_Box_SouthLatitude')
            west = self.reader.parser.doc.get('Bounding_Box_WestLongitude')
            west = convert_to_lon_180(west)
            north = self.reader.parser.doc.get('Bounding_Box_NorthLatitude')
            east = self.reader.parser.doc.get('Bounding_Box_EastLongitude')
            east = convert_to_lon_180(east)
            # bbox: minx=west, miny=south, maxx=east, maxy=north
            geometry = shapely.geometry.box(west, south, east, north)
        except Exception:
            geometry = None
        return geometry
