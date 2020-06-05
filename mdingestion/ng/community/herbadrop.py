from ..schema import JSON


class Herbadrop(JSON):

    @property
    def title(self):
        return self.find('metadata."aip.dc.title".lat')

    @property
    def notes(self):
        text = []
        text.append("Scanned files by OCR.")
        text.append(self.find('images[*].ocr.lat', one=True))
        return text

    @property
    def tags(self):
        return self.find('metadata."aip.dc.subject".lat')

    @property
    def url(self):
        return self.find('metadata."aip.meta.producerIdentifier"', one=True)

    @property
    def related_identifier(self):
        return self.find('additionalIdentifiers.ARK')

    @property
    def metadata_access(self):
        return self.find('depositIdentifier')

    @property
    def author(self):
        return self.find('metadata."aip.dc.creator"')

    @property
    def publisher(self):
        return self.find('metadata."aip.dc.publisher"')

    @property
    def contributor(self):
        return ['CINES']

    @property
    def publication_year(self):
        return self.find('metadata."aip.meta.archivingDate"', type='date_year')

    @property
    def rights(self):
        return self.find('metadata."aip.dc.rights".und')

    @property
    def contact(self):
        return self.publisher

    @property
    def open_access(self):
        return 'true'

    @property
    def language(self):
        return self.find('metadata."aip.dc.language"')

    @property
    def resource_type(self):
        return self.find('metadata."aip.dc.type".eng')

    @property
    def format(self):
        return self.find('metadata."aip.dc.format".eng')

    @property
    def discipline(self):
        return 'Plant Sciences'

    @property
    def spatial_coverage(self):
        return self.find('metadata."aip.dc.coverage".und', one=True)

    @property
    def temporal_coverage_begin(self):
        return self.find('metadata."aip.dc.startDate"', one=True)

    @property
    def temporal_coverage_end(self):
        return self.find('metadata."aip.dc.endDate"', one=True)
