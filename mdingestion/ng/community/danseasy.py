from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class DanseasyDatacite(DataCiteReader):
    NAME = 'danseasy-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        if not doc.doi:
            doc.doi = self.find_doi('alternateIdentifier')
        if not doc.source:
            doc.source = self.find_source('alternateIdentifier')
        doc.discipline = self.discipline(doc)
