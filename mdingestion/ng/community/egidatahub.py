from ..reader import DublinCoreReader


class EGIDatahubDublinCore(DublinCoreReader):
    def update(self, doc):
        doc.publisher = ['EGI Datahub']
        doc.pid = self.pid(doc)

    def pid(self, doc):
        pids = [id for id in self.parser.find('identifier') if id.startswith('http://hdl.handle.net')]
        if pids:
            url = pids[0]
        else:
            url = ''
        return url
