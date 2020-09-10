from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class DarusDatacite(DataCiteReader):
    NAME = 'darus-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.discipline = self.discipline(doc)
