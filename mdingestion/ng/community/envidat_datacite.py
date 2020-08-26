from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class EnvidatDatacite(DataCiteReader):
    NAME = 'envidat-datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'EnviDat'
        doc.discipline = 'Environmental Research'
        if not doc.publisher:
            doc.publisher = ['EnviDat']
