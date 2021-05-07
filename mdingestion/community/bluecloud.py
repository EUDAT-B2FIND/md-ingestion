from .base import Community
from ..service_types import SchemaType, ServiceType


class BluecloudOwnschema(Community):
    NAME = 'bluecloud'
    IDENTIFIER = NAME
    URL = ''
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.API
    OAI_SET = ''
    PRODUCTIVE = False

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
        doc.language = self.find('metadata."aip.dc.language"')
        doc.resource_type = self.find('metadata."aip.dc.type".eng')
        doc.format = self.find('metadata."aip.dc.format".eng')
        doc.size = self.size()
        doc.discipline = 'Plant Sciences'
        doc.instrument = ''
        doc.temporal_coverage_begin_date = self.find('metadata."aip.dc.startDate"')
        doc.temporal_coverage_end_date = self.find('metadata."aip.dc.endDate"')
        doc.spatial_coverage = #bounding box
