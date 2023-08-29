from .base import XMLReader
from ..sniffer import OAISniffer
from ..service_types import SchemaType


class OLACReader(XMLReader):
    SNIFFER = OAISniffer
    SCHEMA = SchemaType.OLAC

    def parse(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('description')
        doc.keywords = self.find('subject')
        doc.discipline = self.discipline(doc)
        doc.doi = self.find_doi('identifier')
        doc.pid = self.find_pid('identifier')
        doc.source = self.find_source('identifier')
        doc.creator = self.find('creator')
        doc.publisher = self.find('publisher')
        doc.contributor = self.find('contributor')
        doc.publication_year = self.find('date')
        doc.rights = self.find('license')
        doc.language = self.find('language')
        doc.resource_type = self.find('type')
        doc.format = self.find('format')
        doc.size = self.find('extent')
        doc.version = self.find('hasVersion')
