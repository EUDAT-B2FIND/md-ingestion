from ..reader import ISO19139Reader
from ..reader import DublinCoreReader


class DeimsISO19139(ISO19139Reader):
    def update(self, doc):
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
        doc.source = self.source(doc)
        doc.related_identifier = self.related_identifier(doc)

    def source(self, doc):
        urls = [id.text for id in self.parser.doc.distributionInfo.find_all('URL') if 'https://deims.org/dataset' in id.text]
        urls.extend([id.text for id in self.parser.doc.distributionInfo.find_all('URL') if 'https://deims.org/network' in id.text])
        return urls

    def related_identifier(self, doc):
        urls = [id.text for id in self.parser.doc.distributionInfo.find_all('URL') if 'https://deims.org/dataset' not in id.text]
        return urls

class DeimsDublicCore(DublinCoreReader):
    def update(self, doc):
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
