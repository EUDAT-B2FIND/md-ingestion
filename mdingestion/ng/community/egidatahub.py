from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class EGIDatahubDublinCore(DublinCoreReader):
    NAME = 'egidatahub-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = ['EGI Datahub']
        doc.pid = self.find_pid('identifier')
        doc.source = self.source(doc)
        # TODO: refactor code
        # doc.keyword = ['EGI']
        keywords = self.find('subject')
        keywords.append('EGI')
        doc.keyword = keywords
        if not doc.publisher:
            doc.publisher = ['EGI Datahub']

    def source(self, doc):
        urls = [url for url in self.find('metadata.identifier') if 'handle' not in url]
        return urls
