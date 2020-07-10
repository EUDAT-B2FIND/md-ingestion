from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class DarusDatacite(DataCiteReader):
    NAME = 'darus-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.discipline = format_value(self.find('subject'), type='string_word')
        doc.open_access = self.find_ok('rights', rightsURI='info:eu-repo/semantics/openAccess')
