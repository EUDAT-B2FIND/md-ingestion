from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class EGIDatahubDublinCore(DublinCoreReader):
    NAME = 'egidatahub-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.publisher = ['EGI Datahub']
        doc.pid = self.find_pid('identifier')
        doc.source = self.source(doc)
        doc.open_access = self.open_access(doc)

    def source(self, doc):
        urls = [url for url in self.find('metadata.identifier') if 'handle' not in url]
        return urls

    def open_access(self, doc):
        for right in ['CC-0', 'https://creativecommons.org/licenses/by-nc-nd/4.0/']:
            if right in doc.rights:
                return True
        return False
