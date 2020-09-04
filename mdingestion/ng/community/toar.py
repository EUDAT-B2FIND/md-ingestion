from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class TOARDublinCore(DublinCoreReader):
    NAME = 'toar-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'B2SHARE'
        # doc.pid = self.find_pid('identifier')
        # doc.source = self.source(doc)
        # doc.keywords = self.keywords(doc)
        doc.publisher = 'Tropospheric Ozone Assessment Report (TOAR)'
        doc.discipline = 'Atmospheric Chemistry'

    def source(self, doc):
        urls = [url for url in self.find('metadata.identifier') if 'handle' not in url]
        return urls
