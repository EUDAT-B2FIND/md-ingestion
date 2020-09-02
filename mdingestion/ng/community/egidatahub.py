from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class EGIDatahubDublinCore(DublinCoreReader):
    NAME = 'egidatahub-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = ['EGI Datahub']
        doc.pid = self.find_pid('identifier')
        doc.source = self.source(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = ['EGI Datahub']

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('EGI')
        return keywords

    def source(self, doc):
        urls = [url for url in self.find('metadata.identifier') if 'handle' not in url]
        return urls
