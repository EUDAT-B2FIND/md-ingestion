from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class UCLDatacite(DataCiteReader):
    NAME = 'ucl-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'University College London'
