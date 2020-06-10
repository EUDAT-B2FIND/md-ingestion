from ..schema import DataCite


class ESSDatacite(DataCite):
    @property
    def doi(self):
        return self.find('identifier', identifierType="URL", one=True)

