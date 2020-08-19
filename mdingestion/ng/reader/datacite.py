import shapely

from .base import XMLReader
from ..sniffer import OAISniffer
from ..format import format_value


class DataCiteReader(XMLReader):
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('description')
        doc.keywords = self.find('subject')
        doc.discipline = format_value(self.find('subject'), type='string_words')
        doc.doi = self.doi()
        doc.related_identifier = self.find('alternateIdentifier')
        doc.creator = self.creator()
        doc.publisher = self.find('publisher')
        doc.contributor = self.find('contributorName')
        doc.funding_reference = self.find('fundingReference.funderName')
        doc.publication_year = self.find('publicationYear')
        doc.rights = self.find('rights')
        doc.contact = doc.creator
        doc.language = self.find('language')
        doc.resource_type = self.find('resourceType')
        doc.format = self.find('format')
        doc.size = self.find('size')
        doc.version = self.find('metadata.version')
        doc.temporal_coverage = self.find('date')
        doc.geometry = self.geometry()
        doc.places = self.find('geoLocationPlace')

    def creator(self):
        creators = []
        for creator in self.parser.doc.find_all('creator'):
            name = creator.creatorName.text
            if creator.affiliation:
                if creator.affiliation.text:
                    name = f"{name} ({creator.affiliation.text})"
            creators.append(name)
        return creators

    def doi(self):
        id = self.find('identifier', identifierType="DOI")
        if not id:
            id = self.find('identifier', identifierType="doi")
        url = f"https://doi.org/{format_value(id, one=True)}"
        return url

    def geometry(self):
        if self.parser.doc.find('geoLocationPoint'):
            # lon = format_value(self.find('geoLocationPoint.pointLongitude'), type='float', one=True)
            # lat = format_value(self.find('geoLocationPoint.pointLatitude'), type='float', one=True)
            # geometry = shapely.geometry.Point(lon, lat)
            point = self.parser.doc.find('geoLocationPoint').text.split()
            # point: x=lon, y=lat
            geometry = shapely.geometry.Point(float(point[0]), float(point[1]))
        elif self.parser.doc.find('geoLocationBox'):
            # west_lon = format_value(self.find('geoLocationBox.westBoundLongitude'), type='float', one=True)
            # east_lon = format_value(self.find('geoLocationBox.eastBoundLongitude'), type='float', one=True)
            # south_lat = format_value(self.find('geoLocationBox.southBoundLatitude'), type='float', one=True)
            # north_lat = format_value(self.find('geoLocationBox.northBoundLatitude'), type='float', one=True)
            # geometry = shapely.geometry.box(west_lon, east_lon, south_lat, north_lat)
            bbox = self.parser.doc.find('geoLocationBox').text.split()
            # bbox: minx=west, miny=south, maxx=east, maxy=north
            geometry = shapely.geometry.box(float(bbox[0]), float(bbox[2]), float(bbox[1]), float(bbox[3]))
        else:
            geometry = None
        return geometry
