from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class PangaeaDatacite(DataCiteReader):
    NAME = 'pangaea-datacite4'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.discipline = 'Earth System Research'
