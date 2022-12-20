from bs4 import BeautifulSoup

from .base import DocParser

import logging


class XMLParser(DocParser):

    def parse_doc(self):
        return BeautifulSoup(open(self.filename), 'xml')

    def find(self, name=None, **kwargs):
        """Just a convienice method for BeautifulSoup doc.find_all()"""
        try:
            if '.' in name:
                # name=something.very.important
                # run: doc.something.very.find_all('important')
                #
                # for tag in doc.find_all("pointOfContact"):
                #   print(tag.CI_ResponsibleParty.individualName.CharacterString.text)
                _dotted, _name = name.rsplit('.', 1)
                _doc = eval(f"doc.{_dotted}", dict(doc=self.doc))
            else:
                _name = name
                _doc = self.doc
            results = [tag.text for tag in _doc.find_all(_name, **kwargs)]
        except Exception:
            logging.warning(f"xml parser failed for name={name}.", exc_info=True)
            results = []
        return results

    @classmethod
    def extension(cls):
        return '.xml'

    @property
    def fulltext(self):
        lines = [txt.strip() for txt in self.doc.find_all(string=True)]
        lines_not_empty = [txt for txt in lines if len(txt) > 0]
        return ','.join(lines_not_empty)
