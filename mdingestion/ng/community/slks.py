from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class SLKSDublinCore(DublinCoreReader):
    NAME = 'slks-dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        # doc.open_access = True
        doc.discipline = 'Archaeology'
