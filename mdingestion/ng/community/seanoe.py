from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class SeanoeDublinCore(DublinCoreReader):
    NAME = 'seanoe-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        # doc.contributor = ['DataverseNO']
        doc.related_identifier = self.find('references')
        doc.discipline = self.discipline(doc, 'Marine Science')
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'SEANOE'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('marine data')
        return keywords
