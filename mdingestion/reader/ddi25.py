from .base import XMLReader
from ..sniffer import OAISniffer


class DDI25Reader(XMLReader):
    """TODO: https://ddialliance.org/resources/ddi-profiles/dc"""
    SNIFFER = OAISniffer

    def parse(self, doc):
        self.identifier(doc)
        doc.title = self.find('titl')
        doc.creator = self.find('AuthEnty')
        self.keywords(doc)
        doc.description = self.find('abstract')
        self.publisher(doc)
        doc.contributor = self.find('othId')
        self.publication_year(doc)
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
                continue
            if 'doi' in URI:
                doc.doi = URI
            elif 'handle' in URI:
                doc.pid = URI
            else:
                doc.source = URI
        if not doc.doi:
            for idno in self.find('IDNo', agency="datacite"):
                doc.doi = idno


    def keywords(self,doc):
        keywords = []
        keywords.extend(self.find('keyword'))
        keywords.extend(self.find('topcClas'))
        doc.keywords = keywords

    def publisher(self,doc):
        publisher = []
        publisher.extend(self.find('producer'))
        if not publisher:
            publisher.extend(self.find('distrbtr'))
        if not publisher:
            publisher.append('CESSDA')
        doc.publisher = publisher

    def publication_year(self,doc):
        distdate = self.parser.doc.find('distDate')
        if distdate:
            doc.publication_year = distdate.get('date') 


