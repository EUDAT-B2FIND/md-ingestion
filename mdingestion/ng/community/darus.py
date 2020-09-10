from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class DarusDatacite(DataCiteReader):
    NAME = 'darus-oai_datacite'
    SNIFFER = OAISniffer
