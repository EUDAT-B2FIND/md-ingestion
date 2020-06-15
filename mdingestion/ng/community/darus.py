from ..schema import DataCite
from ..format import format_string_words


class DarusDatacite(DataCite):
    @property
    def discipline(self):
        return format_string_words(self.find('subject', one=True)).split(' ')[0]
