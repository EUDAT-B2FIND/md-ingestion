from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class DataverseNODublinCore(DublinCoreReader):
    NAME = 'dataverseno-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        # doc.contributor = ['DataverseNO']
        doc.discipline = self.discipline(doc, 'Earth and Environmental Science')
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'DataverseNO'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('EOSC Nordic')
        return keywords
