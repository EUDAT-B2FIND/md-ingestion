from ..reader import DataCiteReader
from ..reader import ISO19139Reader


class EnvidatDatacite(DataCiteReader):
    def update(self, doc):
        doc.contributor = 'EnviDat'


class EnvidatISO19139(ISO19139Reader):
    def update(self, doc):
        doc.contributor = 'EnviDat'
