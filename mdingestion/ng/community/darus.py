from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class DarusDatacite(DataCiteReader):
    NAME = 'darus-oai_datacite'
    SNIFFER = OAISniffer
