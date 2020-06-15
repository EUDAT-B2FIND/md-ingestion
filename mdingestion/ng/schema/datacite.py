import shapely

from .base import OAIMapper
from ..format import format_string_words


class DataCite(OAIMapper):

    @property
    def title(self):
        return self.find('title')

    @property
    def description(self):
        return self.find('description')

    @property
    def tags(self):
        _tags = []
        for subject in self.find('subject'):
            name = format_string_words(subject)
            if name:
                _tags.append(dict(name=name))
        return _tags

    @property
    def source(self):
        return self.find('identifier', identifierType="DOI", one=True)

    @property
    def related_identifier(self):
        return self.find('alternateIdentifier')

    @property
    def creator(self):
        creators = []
        for creator in self.doc.find_all('creator'):
            name = creator.creatorName.text
            if creator.affiliation:
                name = f"{name} ({creator.affiliation.text})"
            creators.append(name)
        return creators

    @property
    def publisher(self):
        return self.find('publisher')

    @property
    def contributor(self):
        return self.find('contributorName')

    @property
    def publication_year(self):
        return self.find('publicationYear')

    @property
    def rights(self):
        return self.find('rights')

    @property
    def contact(self):
        return self.creator

    @property
    def open_access(self):
        return ''

    @property
    def language(self):
        return self.find('language')

    @property
    def resource_type(self):
        return self.find('resourceType')

    @property
    def format(self):
        return self.find('format')

    @property
    def temporal_coverage_begin(self):
        return self.find('date', one=True)

    @property
    def temporal_coverage_end(self):
        return self.temporal_coverage_begin

    def parse_geometry(self):
        if self.doc.find('geoLocationPoint'):
            point = self.doc.find('geoLocationPoint').text.split()
            geometry = shapely.geometry.Point(float(point[0]), float(point[1]))
        elif self.doc.find('geoLocationBox'):
            bbox = self.doc.find('geoLocationBox').text.split()
            geometry = shapely.geometry.box(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
        else:
            geometry = None
        return geometry

    @property
    def doi(self):
        id = self.find('identifier', identifierType="DOI", one=True)
        if not id:
            id = self.find('identifier', identifierType="doi", one=True)
        url = f"https://doi.org/{id}"
        return url
