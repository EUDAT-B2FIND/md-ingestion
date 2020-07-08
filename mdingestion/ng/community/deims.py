from ..reader import ISO19139Reader
from ..reader import DublinCoreReader
from ..format import format_value


class DeimsISO19139(ISO19139Reader):
    def update(self, doc):
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
        doc.source = self.source(doc)
        doc.related_identifier = self.related_identifier(doc)

    def source(self, doc):
        urls = [id for id in self.parser.find('distributionInfo.URL') if 'https://deims.org/dataset' in id]
        urls.extend([id for id in self.parser.find('distributionInfo.URL') if 'https://deims.org/network' in id])
        return urls

    def related_identifier(self, doc):
        urls = [id for id in self.parser.find('distributionInfo.URL') if 'https://deims.org/dataset' not in id]
        return urls


class DeimsDublicCore(DublinCoreReader):
    def update(self, doc):
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
