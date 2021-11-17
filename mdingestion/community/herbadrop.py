from .base import Community
from ..service_types import SchemaType, ServiceType

from ..format import format_value


class Herbadrop(Community):
    NAME = 'herbadrop'
    IDENTIFIER = NAME
    URL = 'https://opendata.cines.fr/herbadrop-api/rest/data/search'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.HERBADROP
    PRODUCTIVE = True

    def update(self, doc):
        doc.title = self.find('metadata."aip.dc.title".lat')
        doc.description = self.description(doc)
        doc.keywords = self.find('metadata."aip.dc.subject".lat')
        doc.pid = self.find('additionalIdentifiers.HANDLE')
        doc.source = self.find('metadata."aip.meta.producerIdentifier"')
        doc.related_identifier = self.find('additionalIdentifiers.ARK')
        doc.metadata_access = self.metadata_access(doc)
        doc.creator = self.find('metadata."aip.dc.creator"')
        doc.publisher = self.find('metadata."aip.dc.publisher"')
        doc.contributor = ['CINES']
        doc.publication_year = self.find('metadata."aip.meta.archivingDate"')
        doc.rights = self.find('metadata."aip.dc.rights".und')
        doc.contact = doc.publisher
        # language is commented out because always undefined "und_UND" - useless!
        # doc.language = self.find('metadata."aip.dc.language"')
        doc.resource_type = self.find('metadata."aip.dc.type".eng')
        doc.format = self.find('metadata."aip.dc.format".eng')
        doc.size = self.size()
        doc.discipline = 'Plant Sciences'
        doc.temporal_coverage_begin_date = self.find('metadata."aip.dc.startDate"')
        doc.temporal_coverage_end_date = self.find('metadata."aip.dc.endDate"')

    def description(self, doc):
        text = []
        text.append("This record is part of a larger collection of digitized herbarium specimens. The full Herbadrop/ICEDIG-project collection can be accessed and browsed at https://opendata.cines.fr")
        image = format_value(self.find('images[*].ocr.lat'), one=True)
        text.append(f"Scanned files by OCR:{image}")
        return text

    def metadata_access(self, doc):
        agency_id = format_value(self.find('transferringAgencyIdentifier'), one=True)
        deposit_id = format_value(self.find('depositIdentifier'), one=True)
        mdaccess = f"https://opendata.cines.fr/herbadrop-api/rest/data/{agency_id}/{deposit_id}"
        return mdaccess

    def spatial_coverage(self, doc):
        return self.find('metadata."aip.dc.coverage".und')

    def size(self):
        value = [f"{size} bytes" for size in self.find('metadata."aip.files"[*].sizeInBytes')]
        return value
