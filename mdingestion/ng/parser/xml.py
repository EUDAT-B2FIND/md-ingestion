from bs4 import BeautifulSoup

from .base import DocParser
from .. import util
from .. import format


class XMLParser(DocParser):

    def parse_doc(self):
        return BeautifulSoup(open(self.filename), 'xml')

    def find(self, name=None, type=None, one=False, **kwargs):
        tags = self.doc.find_all(name, **kwargs)
        formatted = [format.format(tag.text, type=type) for tag in tags]
        formatted = [text.strip() for text in formatted if text.strip()]
        formatted = util.remove_duplicates_from_list(formatted)
        if one:
            if formatted:
                value = formatted[0]
            else:
                value = ''
        else:
            value = formatted
        return value

    @classmethod
    def extension(cls):
        return '.xml'

    @property
    def fulltext(self):
        lines = [txt.strip() for txt in self.doc.find_all(string=True)]
        lines_not_empty = [txt for txt in lines if len(txt) > 0]
        return ','.join(lines_not_empty)
