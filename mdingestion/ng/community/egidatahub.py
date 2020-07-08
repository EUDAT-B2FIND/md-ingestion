from ..reader import DublinCoreReader


class EGIDatahubDublinCore(DublinCoreReader):
    def update(self, doc):
        doc.publisher = ['EGI Datahub']
        doc.pid = self.pid(doc)
        doc.source = self.source(doc)
        doc.open_access = self.open_access(doc)

    def pid(self, doc):
        urls = [id for id in self.parser.find('identifier') if id.startswith('http://hdl.handle.net')]
        return urls

    def source(self, doc):
        urls = [url for url in self.parser.find('metadata.identifier') if 'handle' not in url]
        return urls

    def open_access(self, doc):
        for right in ['CC-0', 'https://creativecommons.org/licenses/by-nc-nd/4.0/']:
            if right in doc.rights:
                return True
        return False
