from .base import XMLReader
from ..sniffer import OAISniffer


class DDIReader(XMLReader):
    """TODO: https://ddialliance.org/resources/ddi-profiles/dc"""
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.community = self.community
        doc.title = self.find('titl')
        doc.source = self.find('sources')
