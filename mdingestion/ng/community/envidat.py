from ..reader import (
    DataCiteReader,
)
from ..sniffer import (
    OAISniffer,
)


class EnvidatDatacite(DataCiteReader):
    NAME = 'envidat-datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'EnviDat'
