import shapely

from .base import XMLReader
from ..sniffer import OAISniffer
from ..format import format_value
from ..util import convert_to_lon_180
from ..service_types import SchemaType


class EudatcoreReader(XMLReader):
    SNIFFER = OAISniffer
    SCHEMA = SchemaType.Eudatcore

    def parse(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('description')
        doc.doi = self.find_doi('identifier', identifierType="DOI")
        doc.pid = self.find_pid('identifier', identifierType="PID")
        doc.source = self.find_source('identifier', identifierType="URL")
        doc.keywords = self.find('keyword')
        doc.discipline = self.find('discipline')
        doc.related_identifier = self.find('relatedIdentifier')
        doc.creator = self.find('creator')
        doc.publisher = self.find('publisher')
        doc.contributor = self.find('contributor')
        doc.funding_reference = self.find('fundingReference')
        doc.publication_year = self.find('publicationYear')
        doc.rights = self.find('rights')
        doc.contact = self.find('contact')
        doc.language = self.find('language')
        doc.resource_type = self.find('resourceType')
        doc.format = self.find('format')
        doc.size = self.find('size')
        doc.version = self.find('version')
        doc.instrument = self.find('instrument')
        doc.temporal_coverage = self.find('temporalCoverage')
        doc.geometry = self.find_geometry()
        doc.places = self.find('geoLocationPlace')

    def geometry(self):
        """
        parse eudatcore geometry based on datacite for now.
        https://schema.datacite.org/meta/kernel-4.3/doc/DataCite-MetadataKernel_v4.3.pdf
        https://guidelines.openaire.eu/en/latest/data/field_geolocation.html
        """
        if self.find('geoLocationPoint.pointLongitude'):
            lon = format_value(self.find('geoLocationPoint.pointLongitude'), type='float', one=True)
            lon = convert_to_lon_180(lon)
            lat = format_value(self.find('geoLocationPoint.pointLatitude'), type='float', one=True)
            # point: x=lon, y=lat
            geometry = shapely.geometry.Point(lon, lat)
        elif self.parser.doc.find('geoLocationPoint'):
            point = self.parser.doc.find('geoLocationPoint').text.split()
            # print(point)
            lat = float(point[0])
            lon = float(point[1])
            lon = convert_to_lon_180(lon)
            # point: x=lon, y=lat
            geometry = shapely.geometry.Point(lon, lat)
        elif self.find('geoLocationBox.westBoundLongitude'):
            west = format_value(self.find('geoLocationBox.westBoundLongitude'), type='float', one=True)
            west = convert_to_lon_180(west)
            east = format_value(self.find('geoLocationBox.eastBoundLongitude'), type='float', one=True)
            east = convert_to_lon_180(east)
            south = format_value(self.find('geoLocationBox.southBoundLatitude'), type='float', one=True)
            north = format_value(self.find('geoLocationBox.northBoundLatitude'), type='float', one=True)
            # print(f"{west} {east} {south} {north}")
            # bbox: minx=west, miny=south, maxx=east, maxy=north
            # print(f"{west}w, {east}e, {south}s, {north}n")
            geometry = shapely.geometry.box(west, south, east, north)
        elif self.parser.doc.find('geoLocationBox'):
            # print('wrong location')
            bbox = self.parser.doc.find('geoLocationBox').text.split()
            # print(bbox)
            south = float(bbox[0])
            west = float(bbox[1])
            west = convert_to_lon_180(west)
            north = float(bbox[2])
            east = float(bbox[3])
            east = convert_to_lon_180(east)
            # bbox: minx=west, miny=south, maxx=east, maxy=north
            geometry = shapely.geometry.box(west, south, east, north)
        elif self.parser.doc.find('geoLocationPolygon'):
            polygon_points = self.parser.doc.geoLocationPolygon.find_all('polygonPoint')
            points = []
            for point in polygon_points:
                lon = float(point.pointLongitude.text)
                lon = convert_to_lon_180(lon)
                lat = float(point.pointLatitude.text)
                points.append((lon, lat))
            geometry = shapely.geometry.Polygon(points)
        else:
            geometry = None
        return geometry
