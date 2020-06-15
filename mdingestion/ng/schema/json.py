from .base import JSONMapper


class JSON(JSONMapper):

    @property
    def title(self):
        return ''

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
        return ''

    @property
    def publisher(self):
        return ''

    @property
    def contributor(self):
        return ''

    @property
    def publication_year(self):
        return ''

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
        return ''

    @property
    def temporal_coverage_end(self):
        return ''

    def parse_geometry(self):
        return None
