import shapely

from .base import XMLMapper


class DataCite(XMLMapper):

    @property
    def title(self):
        return self.find('title')

    @property
    def notes(self):
        return self.find('description')

    @property
    def tags(self):
        return self.find('subject')

    @property
    def url(self):
        return self.find('identifier', identifierType="DOI", one=True)

    @property
    def related_identifier(self):
        return self.find('alternateIdentifier')

    @property
    def metadata_access(self):
        return self.find('identifier')

    @property
    def author(self):
        return self.find('creatorName')

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
        return ''

    @property
    def contact(self):
        return ''

    @property
    def open_access(self):
        return ''

    @property
    def language(self):
        return ''

    @property
    def resource_type(self):
        return ''

    @property
    def format(self):
        return ''

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
