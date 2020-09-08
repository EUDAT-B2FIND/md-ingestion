from ..reader import FGDCReader
from ..sniffer import OAISniffer


class PDCFGDC(FGDCReader):
    NAME = 'pdc-fgdc'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'Polar Data Catalogue'
        doc.discipline = self.discipline(doc, 'Environmental Research')
