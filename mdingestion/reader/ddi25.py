from .base import XMLReader
from ..sniffer import OAISniffer
from ..service_types import SchemaType


class DDI25Reader(XMLReader):
    """TODO: https://ddialliance.org/resources/ddi-profiles/dc"""
    SNIFFER = OAISniffer
    SCHEMA = SchemaType.DDI25

    def parse(self, doc):
        self.identifier(doc)
        doc.title = self.find('stdyDscr.titlStmt.titl')
        doc.creator = self.find('AuthEnty')
        self.keywords(doc)
        self.description(doc)
        doc.publisher = self.find('producer')
#        self.publisher(doc)
        doc.contributor = self.find('othId')
        self.publication_year(doc)
        doc.resource_type = self.find('dataKind')
        doc.format = self.format(doc)
        doc.discipline = self.discipline(doc)
        self.related_identifier(doc)
        self.rights(doc)
        self.contact(doc)
        self.language(doc)
        self.temporal_coverage(doc)
#       doc.geometry = self.find_geometry('geogCover')
        self.places(doc)
#       doc.size = self.find('extent')

#       doc.version = self.find('hasVersion')
        doc.funding_reference = self.find('fundAg')
#       doc.instrument = self.find('')

    def identifier(self, doc):
        for holdings in self.parser.doc.find_all('docDscr.holdings'):
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

#    def publisher(self,doc):
#        publisher = []
#       publisher.extend(self.find('producer'))
#        publisher.extend(self.find('distrbtr'))

    def contact(self,doc):
        _contact = self.parser.doc.find('distrbtr')
        if _contact:
            doc.contact = _contact.get('URI')

    def publication_year(self,doc):
        distdate = self.parser.doc.find('distDate')
        if distdate:
            doc.publication_year = distdate.get('date')

    def rights(self,doc):
        _rights = []
        _rights.extend(self.find('copyright'))
        _rights.extend(self.find('restrctn'))
        doc.rights = _rights

    def description(self,doc):
        descr = []
        descr.extend(self.find('abstract'))
        descr.extend(self.find('sampProc'))
        descr.extend(self.find('collMode'))
        doc.description = descr

    def format(self, doc):
        filetypes = self.find('fileType')
        filetypes = [t for t in filetypes if '-' not in t]
        new_filetypes = []
        for t in filetypes:
            new_filetypes.extend(t.split(','))
        return new_filetypes

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
