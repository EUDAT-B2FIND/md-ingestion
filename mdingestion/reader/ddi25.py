from .base import XMLReader
from ..sniffer import OAISniffer


class DDI25Reader(XMLReader):
    """TODO: https://ddialliance.org/resources/ddi-profiles/dc"""
    SNIFFER = OAISniffer

    def parse(self, doc):
        self.identifier(doc)
        doc.title = self.find('titl')
        doc.creator = self.find('AuthEnty')
        doc.keywords = self.find('keyword') #TODO: add method TopcClas
        doc.description = self.find('abstract')
        doc.publisher = self.find('producer') #TODO: method if producer is missing?
        doc.contributor = self.find('othId')
        doc.publication_year = self.find('prodDate')
        doc.resource_type = self.find('dataKind')
        doc.format = self.find('fileType')
        doc.discipline = self.discipline(doc)
        doc.related_identifier = self.find('othrStdyMat')
        doc.rights = self.find('copyright')
        #doc.contact = 
        #doc.language = self.find('') TODO: abstract attribut auslesen?
        # doc.temporal_coverage_begin = self.find('timePrd.date, event=start')
        # doc.temporal_coverage_end = self.find('timePrd.date, event=end')
        #doc.geometry = self.find_geometry('geogCover')
        #doc.places = self.find('geogCover, nation')
        #doc.size = self.find('extent')
        #doc.version = self.find('hasVersion')

    def identifier(self, doc):
        for holdings in self.parser.doc.find_all('holdings'):
            URI = holdings.get('URI')
            if not URI:
                print(self.find('titl'))
                continue
            if 'doi' in URI:
                doc.doi = URI
            elif 'handle' in URI:
                doc.pid = URI
            else:
                doc.source = URI
