import shapely

from .base import XMLReader
from ..sniffer import OAISniffer


class DublinCoreReader(XMLReader):
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('description')
        doc.keywords = self.find('subject')
        doc.doi = self.find_doi('metadata.identifier')
        doc.pid = self.find_pid('metadata.identifier')
        doc.source = self.find_source('metadata.identifier')
        doc.related_identifier = self.find('relation')
        doc.creator = self.find('creator')
        doc.publisher = self.find('publisher')
        doc.contributor = self.find('contributor')
        doc.publication_year = self.find('date')
        doc.rights = self.find('rights')
        doc.contact = doc.publisher
        doc.language = self.find('language')
        doc.resource_type = self.find('type')
        doc.format = self.find('format')
        doc.temporal_coverage_begin = ''
        doc.temporal_coverage_end = ''
        doc.geometry = self.geometry()
        doc.size = self.find('extent')
        doc.version = self.find('hasVersion')

    def geometry(self):
        # <dcterms:spatial xsi:type="dcterms:POINT">9.811246,56.302585</dcterms:spatial>
        point = self.parser.doc.find('spatial', attrs={'xsi:type': 'dcterms:POINT'})
        if point:
            coords = point.text.split(',')
            geometry = shapely.geometry.Point(float(coords[0]), float(coords[1]))
        else:
            geometry = None
        return geometry
