import shapely

from .base import XMLReader
from ..sniffer import OAISniffer

from ..format import format_value


class DataCiteReader(XMLReader):
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.community = self.community
        doc.title = self.find('title')
        doc.description = self.find('description')
        doc.tags = self.find('subject')
        doc.doi = self.doi()
        doc.related_identifier = self.find('alternateIdentifier')
        doc.creator = self.creator()
        doc.publisher = self.find('publisher')
        doc.contributor = self.find('contributorName')
        doc.publication_year = self.find('publicationYear')
        doc.rights = self.find('rights')
        doc.contact = doc.creator
        doc.open_access = False
        doc.language = self.find('language')
        doc.resource_type = self.find('resourceType')
        doc.format = self.find('format')
        doc.temporal_coverage_begin = self.find('date')
        doc.temporal_coverage_end = doc.temporal_coverage_begin
        doc.geometry = self.geometry()

    def creator(self):
        creators = []
        for creator in self.parser.doc.find_all('creator'):
            name = creator.creatorName.text
            if creator.affiliation:
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
            point = self.parser.doc.find('geoLocationPoint').text.split()
            geometry = shapely.geometry.Point(float(point[0]), float(point[1]))
        elif self.parser.doc.find('geoLocationBox'):
            bbox = self.parser.doc.find('geoLocationBox').text.split()
            geometry = shapely.geometry.box(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
        else:
            geometry = None
        return geometry
