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
        doc.pid = self.pid()
        doc.source = self.find_source('resource.identifier')
        doc.keywords = self.find('subject')
        doc.discipline = self.discipline(doc)
        doc.related_identifier = self.find('relatedIdentifier')
        doc.creator = self.creator()
        doc.publisher = self.find('publisher')
        doc.contributor = self.find('contributorName')
        doc.funding_reference = self.funding_reference()
        doc.publication_year = self.publication_year()
        doc.rights = self.rights()
        doc.contact = self.contact()
        doc.language = self.find('language')
        doc.resource_type = self.find('resourceType')
        doc.format = self.find('format')
        doc.size = self.find('size')
        doc.version = self.find('metadata.version')
        doc.temporal_coverage = self.find('date')
        doc.geometry = self.find_geometry()
        doc.places = self.find('geoLocationPlace')

    def pid(self):
        urls = self.find_pid('alternateIdentifier')
        urls.extend(self.find_pid('relatedIdentifier', relatedIdentifierType="Handle"))
        urls.extend(self.find('alternateIdentifier', alternateIdentifierType="URN"))
        return urls

    def publication_year(self):
        year = format_value(self.find('publicationYear'), one=True)
        if not year:
            year = self.find('header.datestamp')
        return year

    def rights(self):
        rights = self.find('rights')
        for right in self.parser.doc.find_all('rights'):
            URI = right.get('rightsURI')
            if URI:
                rights.append(URI)
        return rights

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

    def funding_reference(self):
        funding_reference = self.find('fundingReferences.funderName')
        if not funding_reference:
            funding_reference = self.find('contributor', contributorType="Funder")
        return funding_reference

    def geometry(self):
        """
        parse datacite geometry.
        https://schema.datacite.org/meta/kernel-4.3/doc/DataCite-MetadataKernel_v4.3.pdf
        https://guidelines.openaire.eu/en/latest/data/field_geolocation.html
        """
        if self.find('geoLocationPoint.pointLongitude'):
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
        elif self.find('geoLocationBox.westBoundLongitude'):
            west = format_value(self.find('geoLocationBox.westBoundLongitude'), type='float', one=True)
            east = format_value(self.find('geoLocationBox.eastBoundLongitude'), type='float', one=True)
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
