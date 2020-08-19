from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class RadarDatacite(DataCiteReader):
    NAME = 'radar-datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'Radar'
        doc.related_identifier = self.related_identifier(doc)

    def related_identifier(self, doc):
        ids = self.find('relatedIdentifier')
        new_ids = []
        for val in ids:
            if val.startswith('urn:'):
                val = f"http://nbn-resolving.de/{val}"
            new_ids.append(val)
        return new_ids
