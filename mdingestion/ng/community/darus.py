from ..reader import DataCiteReader
from ..format import format_value


class DarusDatacite(DataCiteReader):
    def update(self, doc):
        doc.discipline = format_value(self.parser.find('subject'), type='string_words', one=True).split(' ')[0]
        doc.open_access = self.open_access(doc)

    def open_access(self, doc):
        if self.parser.find('rights', rightsURI='info:eu-repo/semantics/openAccess'):
            return True
        return False
