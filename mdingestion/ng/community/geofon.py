from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class GeofonDatacite(DataCiteReader):
    NAME = 'geofon-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.discipline = 'Seismology'
