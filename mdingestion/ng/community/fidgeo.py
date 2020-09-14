from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class DarusDatacite(DataCiteReader):
    NAME = 'fidgeo-oai_datacite'
    SNIFFER = OAISniffer
