from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class ESSDatacite(DataCiteReader):
    NAME = 'ess-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.doi = self.find_doi('identifier', identifierType="URL")
        doc.discipline = 'Particles, Nuclei and Fields'
        # doc.open_access = True
