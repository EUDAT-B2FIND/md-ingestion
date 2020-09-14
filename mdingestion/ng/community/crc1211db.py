from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class CRC1211DBDatacite(DataCiteReader):
    NAME = 'crc1211db-oai_datacite'
    SNIFFER = OAISniffer
