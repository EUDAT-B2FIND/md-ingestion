import shapely

from .base import XMLReader
from ..sniffer import OAISniffer
from ..format import format_value


class FFReader(XMLReader):
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.title = self.find('name')
        # doc.description = self.find('description')
        # doc.doi = self.find_doi('resource.identifier')
        # doc.pid = self.find_pid('alternateIdentifier')
        doc.source = self.find_source('site.id')
        keywords = self.find('placeName.term')
        keywords.extend(self.find('objectType.term'))
        doc.keywords = keywords
        # doc.discipline = self.discipline(doc)
        doc.related_identifier = []
        doc.creator = self.find('supervision.term')
        # doc.publisher = self.find('publisher')
        doc.contributor = self.find('museum.term')
        # doc.funding_reference = self.find('fundingReference.funderName')
        doc.publication_year = self.find('site.date')
        # doc.rights = self.find('rights')
        # doc.contact = self.contact()
        # doc.language = self.find('language')
        # doc.resource_type = self.find('resourceType')
        # doc.format = self.find('format')
        # doc.size = self.find('size')
        # doc.version = self.find('metadata.version')
        doc.temporal_coverage = self.find('primaryObject.mainPeriod')
        doc.temporal_coverage_begin_date = self.find('primaryObject.date.fromYear')
        doc.temporal_coverage_end_date = self.find('primaryObject.date.toYear')
        doc.geometry = self.find_geometry()
        doc.places = self.find('geoLocationPlace')

    def geometry(self):
        """
        parse datacite geometry.

        https://guidelines.openaire.eu/en/latest/data/field_geolocation.html
        """
        if self.parser.doc.find('geoLocationPoint.pointLongitude'):
            lon = format_value(self.find('geoLocationPoint.pointLongitude'), type='float', one=True)
            lat = format_value(self.find('geoLocationPoint.pointLatitude'), type='float', one=True)
            # point: x=lon, y=lat
            geometry = shapely.geometry.Point(lon, lat)
        elif self.parser.doc.find('geoLocationPoint'):
            point = self.parser.doc.find('geoLocationPoint').text.split()
            # print(point)
            lat = float(point[0])
            lon = float(point[1])
            # point: x=lon, y=lat
            geometry = shapely.geometry.Point(lon, lat)
        elif self.parser.doc.find('geoLocationBox.westBoundLongitude'):
            west = format_value(self.find('geoLocationBox.westBoundLongitude'), type='float', one=True)
            east = format_value(self.find('geoLocationBox.eastBoundLongitude'), type='float', one=True)
            south = format_value(self.find('geoLocationBox.southBoundLatitude'), type='float', one=True)
            north = format_value(self.find('geoLocationBox.northBoundLatitude'), type='float', one=True)
            # print(f"{west} {east} {south} {north}")
            # bbox: minx=west, miny=south, maxx=east, maxy=north
            geometry = shapely.geometry.box(west, south, east, north)
        elif self.parser.doc.find('geoLocationBox'):
            bbox = self.parser.doc.find('geoLocationBox').text.split()
            # print(bbox)
            south = float(bbox[0])
            west = float(bbox[1])
            north = float(bbox[2])
            east = float(bbox[3])
            # bbox: minx=west, miny=south, maxx=east, maxy=north
            geometry = shapely.geometry.box(west, south, east, north)
        elif self.parser.doc.find('geoLocationPolygon'):
            polygon_points = self.parser.doc.geoLocationPolygon.find_all('polygonPoint')
            points = []
            for point in polygon_points:
                points.append((float(point.pointLongitude.text), float(point.pointLatitude.text)))
            geometry = shapely.geometry.Polygon(points)
        else:
            geometry = None
        return geometry
