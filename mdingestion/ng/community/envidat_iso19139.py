from ..reader import ISO19139Reader
from ..sniffer import OAISniffer
from ..format import format_value


class EnviDatISO19139(ISO19139Reader):
    NAME = 'envidat-iso19139'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.doi = self.find_doi('MD_Metadata.fileIdentifier')
        doc.pid = self.find_pid('MD_Metadata.fileIdentifier')
        doc.source = self.find_source('linkage')
        doc.discipline = self.discipline(doc, 'Environmental Research')
