from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class CERICDatacite(DataCiteReader):
    NAME = 'ceric-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.open_access = True
        doc.doi = ""
        doc.source = self.find("metadata.identifier", identifierType='URL')
