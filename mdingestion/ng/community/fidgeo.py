from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class FidgeoDatacite(DataCiteReader):
    NAME = 'fidgeo-oai_datacite'
    SNIFFER = OAISniffer
