from bs4 import BeautifulSoup

from .base import DocParser
from .. import util
from .. import format


class XMLParser(DocParser):

    def parse_doc(self):
        return BeautifulSoup(open(self.filename), 'xml')

    def find(self, name=None, **kwargs):
        """Just a convienice method for BeautifulSoup doc.find_all()"""
        return [tag.text for tag in self.doc.find_all(name, **kwargs)]

    @classmethod
    def extension(cls):
        return '.xml'

    @property
    def fulltext(self):
        lines = [txt.strip() for txt in self.doc.find_all(string=True)]
        lines_not_empty = [txt for txt in lines if len(txt) > 0]
        return ','.join(lines_not_empty)
