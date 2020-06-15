import shapely

from .base import OAIMapper
from ..format import format_string_words


class DublinCore(OAIMapper):

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
        return self.find('identifier', one=True)

    @property
    def related_identifier(self):
        return self.find('relation')

    @property
    def metadata_access(self):
        return self.find('header/identifier')

    @property
    def creator(self):
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
        # TODO: bugfix rights not found
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
        point = self.doc.find('spatial', attrs={'xsi:type': 'dcterms:POINT'})
        if point: 
            coords = point.text.split(',')
            geometry = shapely.geometry.Point(float(coords[0]), float(coords[1]))
        else: 
            geometry = None
        return geometry

    @property
    def temporal_coverage_begin(self):
        return ''

    @property
    def temporal_coverage_end(self):
        return ''

    @property
    def doi(self):
        # TODO: deal with generic DOI recognition
        url = f"https://doi.org/{self.oai_identifier[0]}"
        return url
