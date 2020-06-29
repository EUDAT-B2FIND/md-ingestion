from ..reader import JSONReader
from ..format import format_value


class Herbadrop(JSONReader):
    def update(self, doc):
        doc.title = self.parser.find('metadata."aip.dc.title".lat')
        doc.description = self.description(doc)
        doc.tags = self.parser.find('metadata."aip.dc.subject".lat')
        doc.pid = self.parser.find('additionalIdentifiers.HANDLE')
        doc.source = self.parser.find('metadata."aip.meta.producerIdentifier"')
        doc.related_identifier = self.parser.find('additionalIdentifiers.ARK')
        doc.metadata_access = self.metadata_access(doc)
        doc.creator = self.parser.find('metadata."aip.dc.creator"')
        doc.publisher = self.parser.find('metadata."aip.dc.publisher"')
        doc.contributor = ['CINES']
        doc.publication_year = self.parser.find('metadata."aip.meta.archivingDate"')
        doc.rights = self.parser.find('metadata."aip.dc.rights".und')
        doc.contact = doc.publisher
        doc.open_access = ['true']
        doc.language = self.parser.find('metadata."aip.dc.language"')
        doc.resource_type = self.parser.find('metadata."aip.dc.type".eng')
        doc.format = self.parser.find('metadata."aip.dc.format".eng')
        doc.discipline = 'Plant Sciences'
        doc.temporal_coverage_begin = self.parser.find('metadata."aip.dc.startDate"')
        doc.temporal_coverage_end = self.parser.find('metadata."aip.dc.endDate"')

    def description(self, doc):
        text = []
        text.append("""Herbadrop is a project that blablubs did a lot of fancy stuff,
        some records are searchable and findable in B2FIND now, but this is
        only a part of a larger collection.""")
        # text.append("Scanned files by OCR.")
        # text.append(self.parser.find('images[*].ocr.lat'))
        return text

    def metadata_access(self, doc):
        agency_id = format_value(self.parser.find('transferringAgencyIdentifier'), one=True)
        deposit_id = format_value(self.parser.find('depositIdentifier'), one=True)
        mdaccess = f"https://opendata.cines.fr/herbadrop-api/rest/data/{agency_id}/{deposit_id}"
        return mdaccess

    def spatial_coverage(self, doc):
        self.parser.find('metadata."aip.dc.coverage".und')
