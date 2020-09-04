import shapely

from .base import XMLReader
from ..sniffer import OAISniffer
from ..format import format_value


class DataCiteReader(XMLReader):
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('description')
        doc.doi = self.find_doi('resource.identifier')
        doc.pid = self.find_pid('alternateIdentifier')
        doc.source = self.find_source('resource.identifier')
        doc.keywords = self.find('subject')
        doc.discipline = format_value(self.find('subject'), type='string_words')
        doc.related_identifier = self.find('relatedIdentifier')
        doc.creator = self.creator()
        doc.publisher = self.find('publisher')
        doc.contributor = self.find('contributorName')
        doc.funding_reference = self.find('fundingReference.funderName')
        doc.publication_year = self.find('publicationYear')
        doc.rights = self.find('rights')
        doc.contact = self.contact()
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
                affiliation = format_value(creator.affiliation.text, one=True)
                if affiliation:
                    name = f"{name} ({affiliation})"
            creators.append(name)
        return creators

    def contact(self):
        contacts = []
        for contact in self.parser.doc.find_all('contributor', contributorType="ContactPerson"):
            name = format_value(contact.contributorName.text, type='email', one=True)
            if contact.affiliation:
                affiliation = format_value(contact.affiliation.text, one=True)
                if affiliation:
                    name = f"{name} ({affiliation})"
            contacts.append(name)
        return contacts

    def geometry(self):
        if self.parser.doc.find('geoLocationPoint'):
            lon = format_value(self.find('geoLocationPoint.pointLongitude'), type='float', one=True)
            lat = format_value(self.find('geoLocationPoint.pointLatitude'), type='float', one=True)
            # point: x=lon, y=lat
            geometry = shapely.geometry.Point(lon, lat)
        elif self.parser.doc.find('geoLocationBox'):
            west = format_value(self.find('geoLocationBox.westBoundLongitude'), type='float', one=True)
            east = format_value(self.find('geoLocationBox.eastBoundLongitude'), type='float', one=True)
            south = format_value(self.find('geoLocationBox.southBoundLatitude'), type='float', one=True)
            north = format_value(self.find('geoLocationBox.northBoundLatitude'), type='float', one=True)
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
