from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class RadarDatacite(DataCiteReader):
    NAME = 'radar-datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'Radar'
