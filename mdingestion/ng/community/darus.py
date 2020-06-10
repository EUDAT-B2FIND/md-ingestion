from ..schema import DataCite


class DarusDatacite(DataCite):
    @property
    def discipline(self):
        return self.find('subject')
