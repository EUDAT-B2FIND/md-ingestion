from ..reader import ISO19139Reader
from ..reader import DublinCoreReader


class DeimsISO19139(ISO19139Reader):
    def update(self, doc):
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'


class DeimsDublicCore(DublinCoreReader):
    def update(self, doc):
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
