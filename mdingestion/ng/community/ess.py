from ..reader import DataCiteReader


class ESSDatacite(DataCiteReader):
    def update(self, doc):
        doc.doi = self.parser.find('identifier', identifierType="URL")
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.open_access = 'true'
