from ..reader import (
    DataCiteReader,
    ISO19139Reader,
)
from ..sniffer import (
    OAISniffer,
    CSWSniffer,
)


class EnvidatDatacite(DataCiteReader):
    NAME = 'envidat-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'EnviDat'


class EnvidatISO19139(ISO19139Reader):
    NAME = 'envidat-iso19139'
    SNIFFER = CSWSniffer

    def update(self, doc):
        doc.contributor = 'EnviDat'
