from ..schema import DublinCore


class SLKSDublinCore(DublinCore):
    @property
    def open_access(self):
        return 'true'

    @property
    def discipline(self):
        return 'Archaeology'
