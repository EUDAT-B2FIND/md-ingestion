from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class EnvidatDatacite(DataCiteReader):
    NAME = 'envidat-datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.source = self.find_source('alternateIdentifier')
        doc.contact = 'envidat@wsl.ch'
        if not doc.publisher:
            doc.publisher = 'EnviDat'
        doc.contributor = self.contributor(doc)
        doc.discipline = self.discipline(doc, 'Environmental Research')

    def contributor(self, doc):
        contributor = [name for name in doc.contributor if name not in doc.contact]
        contributor.append('EnviDat')
        return contributor
