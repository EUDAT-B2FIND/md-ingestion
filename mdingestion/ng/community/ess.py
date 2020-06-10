from ..schema import DataCite


class ESSDatacite(DataCite):
    @property
    def doi(self):
        return self.find('identifier', identifierType="URL", one=True)

    @property
    def discipline(self):
        return 'Particles, Nuclei and Fields'

