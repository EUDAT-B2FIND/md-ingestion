import shapely

from .base import XMLReader
from ..sniffer import OAISniffer


class DublinCoreReader(XMLReader):
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('description')
        doc.keyword = self.find('subject')
        # doc.doi = f"https://doi.org/{doc.oai_identifier[0]}"
        # doc.source = self.find('identifier')
        doc.related_identifier = self.find('relation')
        doc.creator = self.find('creator')
        doc.publisher = self.find('publisher')
        doc.contributor = self.find('contributor')
        doc.publication_year = self.find('date')
        doc.rights = self.find('rights')
        doc.contact = doc.publisher
        doc.open_access = True
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
