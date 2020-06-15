import shapely

from .base import XMLMapper


class ISO19139(XMLMapper):

    @property
    def title(self):
        return self.find('title')

    @property
    def description(self):
        return ''

    @property
    def tags(self):
        return ''

    @property
    def source(self):
        return ''

    @property
    def related_identifier(self):
        return ''

    @property
    def metadata_access(self):
        return ''

    @property
    def creator(self):
        return self.find('individualName')

    @property
    def publisher(self):
        return self.find('organisationName')

    @property
    def contributor(self):
        return ''

    @property
    def publication_year(self):
        return self.find('CI_Date', type='date_year')

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
        if self.doc.find('EX_GeographicBoundingBox'):
            bbox = self.doc.find('EX_GeographicBoundingBox').text.split()
            geometry = shapely.geometry.box(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
        else:
            geometry = None
        return geometry
