import shapely

from .base import XMLMapper


class DublinCore(XMLMapper):

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
        return self.find('identifier', one=True)

    @property
    def related_identifier(self):
        return self.find('relation')

    @property
    def metadata_access(self):
        return self.find('header/identifier')

    @property
    def author(self):
        return self.find('creator')

    @property
    def contributor(self):
        return self.find('contributor')

    @property
    def publisher(self):
        return self.find('publisher')

    @property
    def publication_year(self):
        return self.find('date', type='date_year')

    @property
    def rights(self):
        return self.find('rigths')

    @property
    def contact(self):
        return self.publisher

    @property
    def open_access(self):
        return ''

    @property
    def language(self):
        return self.find('language')

    @property
    def resource_type(self):
        return self.find('type')

    @property
    def format(self):
        return self.find('format')

    def parse_geometry(self):
        # <dcterms:spatial xsi:type="dcterms:POINT">9.811246,56.302585</dcterms:spatial>
        coords = self.doc.find('spatial', attrs={'xsi:type': 'dcterms:POINT'}).text.split(',')
        geometry = shapely.geometry.Point(float(coords[0]), float(coords[1]))
        return geometry

    @property
    def temporal_coverage_begin(self):
        return ''

    @property
    def temporal_coverage_end(self):
        return ''
