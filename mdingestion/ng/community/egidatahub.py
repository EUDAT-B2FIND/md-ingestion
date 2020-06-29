from ..reader import DublinCoreReader


class EGIDatahubDublinCore(DublinCoreReader):
    def update(self, doc):
        doc.publisher = ['EGI Datahub']
        doc.pid = self.pid(doc)
        doc.source = self.source(doc)
        doc.open_access = self.open_access(doc)

    def pid(self, doc):
        pids = [id for id in self.parser.find('identifier') if id.startswith('http://hdl.handle.net')]
        if pids:
            url = pids[0]
        else:
            url = ''
        return url

    def source(self, doc):
        urls = [id.text for id in self.parser.doc.metadata.find_all('identifier') if not 'handle' in id.text]
        if urls:
            url = urls[0]
        else:
            url = ''
        return url

    def open_access(self, doc):
        for right in ['CC-0', 'https://creativecommons.org/licenses/by-nc-nd/4.0/']:
            if right in doc.rights:
                return True
        return False
