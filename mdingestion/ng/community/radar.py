from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class RadarDublinCore(DublinCoreReader):
    NAME = 'radar-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'Radar'
