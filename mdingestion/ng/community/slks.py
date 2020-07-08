from ..reader import DublinCoreReader


class SLKSDublinCore(DublinCoreReader):
    def update(self, doc):
        doc.open_access = ['true']
        doc.discipline = 'Archaeology'
