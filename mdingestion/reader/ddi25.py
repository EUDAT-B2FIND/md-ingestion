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
        self.related_identifier(doc)
        self.rights(doc)
        #doc.contact = 
        self.language(doc)
        self.temporal_coverage(doc)
        #doc.geometry = self.find_geometry('geogCover')
        self.places(doc)
        #doc.size = self.find('extent')
        #doc.version = self.find('hasVersion')
        doc.funding_reference = self.find('fundAg')
        self.instrument(doc)

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

    def related_identifier(self,doc):
        related_ids = []
        related_ids.extend(self.find('othrStdyMat'))
        related_ids.extend(self.find('sources'))
        doc.related_identifier = related_ids

    def keywords(self,doc):
        _keywords = []
        _keywords.extend(self.find('keyword'))
        _keywords.extend(self.find('topcClas'))
        doc.keywords = _keywords

    def language(self,doc):
        langs = []
        for holdings in self.parser.doc.find_all('holdings'):
            langs.append(holdings.get('xml:lang'))
        doc.language = langs

    def publisher(self,doc):
        publisher = []
        #publisher.extend(self.find('producer'))
        publisher.extend(self.find('distrbtr'))
        if not publisher:
            publisher.append('CESSDA')
        doc.publisher = publisher

    def publication_year(self,doc):
        distdate = self.parser.doc.find('distDate')
        if distdate:
            doc.publication_year = distdate.get('date')

    def rights(self,doc):
        _rights = []
        _rights.extend(self.find('copyright'))
        _rights.extend(self.find('restrctn'))
        doc.rights = _rights

    def instrument(self,doc):
        instrs = []
        instrs.extend(self.find('sampProc'))
        keywords.extend(self.find('collMode'))
        doc.instrument = instrs

    def temporal_coverage(self,doc):
        tempbegin = self.parser.doc.find('timePrd', event="start")
        if tempbegin:
            doc.temporal_coverage_begin_date = tempbegin.get('date')
        tempend = self.parser.doc.find('timePrd', event="end")
        if tempend:
            doc.temporal_coverage_end_date = tempend.get('date')

    def places(self,doc):
        places = []
        places.extend(self.find('geogCover'))
        places.extend(self.find('nation'))
        doc.places = places



