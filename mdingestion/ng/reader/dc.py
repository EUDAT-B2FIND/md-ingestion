import shapely

from .base import OAIReader


class DublinCoreReader(OAIReader):

    def parse(self, doc):
        doc.title = self.parser.find('title')
        doc.description = self.parser.find('description')
        doc.tags = self.parser.find('subject')
        doc.doi = f"https://doi.org/{doc.oai_identifier[0]}"
        doc.source = self.parser.find('identifier')
        doc.related_identifier = self.parser.find('relation')
        doc.creator = self.parser.find('creator')
        doc.publisher = self.parser.find('publisher')
        doc.contributor = self.parser.find('contributor')
        doc.publication_year = self.parser.find('date')
        doc.rights = self.parser.find('rights')
        doc.contact = doc.publisher
        doc.open_access = ''
        doc.language = self.parser.find('language')
        doc.resource_type = self.parser.find('type')
        doc.format = self.parser.find('format')
        doc.temporal_coverage_begin = ''
        doc.temporal_coverage_end = ''
        doc.geometry = self.geometry(doc)

    def geometry(self, doc):
        # <dcterms:spatial xsi:type="dcterms:POINT">9.811246,56.302585</dcterms:spatial>
        point = self.parser.doc.find('spatial', attrs={'xsi:type': 'dcterms:POINT'})
        if point:
            coords = point.text.split(',')
            geometry = shapely.geometry.Point(float(coords[0]), float(coords[1]))
        else:
            geometry = None
        return geometry
