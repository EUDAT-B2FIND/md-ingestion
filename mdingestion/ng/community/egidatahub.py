from ..schema import DublinCore

class EGIDatahubDublinCore(DublinCore):

    @property
    def publisher(self):
        return ['EGI Datahub']