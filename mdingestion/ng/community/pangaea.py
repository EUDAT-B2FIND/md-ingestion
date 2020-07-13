from ..reader import DataCiteReader
from ..sniffer import OAISniffer


class PangaeaDatacite(DataCiteReader):
    NAME = 'pangaea-datacite4'
    SNIFFER = OAISniffer

    def update(self, doc):
        # doc.discipline = format_value(self.find('subject'), type='string_words', one=True).split(' ')[0]
        pass
