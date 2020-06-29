import shapely

from .base import OAIReader

from ..format import format_value


class DataCiteReader(OAIReader):

    def parse(self, doc):
        doc.title = self.parser.find('title')
        doc.description = self.parser.find('description')
        doc.tags = self.parser.find('subject')
        doc.doi = self.doi(doc)
        doc.source = doc.doi
        doc.related_identifier = self.parser.find('alternateIdentifier')
        doc.creator = self.creator(doc)
        doc.publisher = self.parser.find('publisher')
        doc.contributor = self.parser.find('contributorName')
        doc.publication_year = self.parser.find('publicationYear')
        doc.rights = self.parser.find('rights')
        doc.contact = doc.creator
        doc.open_access = ''
        doc.language = self.parser.find('language')
        doc.resource_type = self.parser.find('resourceType')
        doc.format = self.parser.find('format')
        doc.temporal_coverage_begin = self.parser.find('date')
        doc.temporal_coverage_end = doc.temporal_coverage_begin
        doc.geometry = self.geometry(doc)

    def creator(self, doc):
        creators = []
        for creator in self.parser.doc.find_all('creator'):
            name = creator.creatorName.text
            if creator.affiliation:
                name = f"{name} ({creator.affiliation.text})"
            creators.append(name)
        return creators

    def doi(self, doc):
        id = self.parser.find('identifier', identifierType="DOI")
        if not id:
            id = self.parser.find('identifier', identifierType="doi")
        url = f"https://doi.org/{format_value(id, one=True)}"
        return url

    def geometry(self, doc):
        if self.parser.doc.find('geoLocationPoint'):
            point = self.parser.doc.find('geoLocationPoint').text.split()
            geometry = shapely.geometry.Point(float(point[0]), float(point[1]))
        elif self.parser.doc.find('geoLocationBox'):
            bbox = self.parser.doc.find('geoLocationBox').text.split()
            geometry = shapely.geometry.box(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
        else:
            geometry = None
        return geometry
