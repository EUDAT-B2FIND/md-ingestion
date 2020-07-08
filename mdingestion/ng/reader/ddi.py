from .base import XMLReader, OAISniffer


class DDIReader(XMLReader):
    """TODO: https://ddialliance.org/resources/ddi-profiles/dc"""
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.title = self.parser.find('titl')
        doc.source = self.parser.find('sources')
