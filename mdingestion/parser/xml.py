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
                # path = pointOfContact.CI_ResponsibleParty.individualName.CharacterString.text
                
                # for tag in doc.find_all("pointOfContact"):
                #   print(tag.CI_ResponsibleParty.individualName.CharacterString.text)
                tags = name.split('.')
                first = tags[0]
                last = tags[-1]
                
                if len(tags) > 2:
                    dotted = '.'.join(tags[1: -2])
                else:
                    dotted = None
                results = []
                for tag in self.doc.find_all(first):
                    if dotted:
                        _doc = eval(f"doc.{dotted}", dict(doc=tag))
                    else:
                        _doc = tag
                    _tags = [_tag.text for _tag in _doc.find_all(last, **kwargs)]
                    results.extend(_tags)
            else:
                results = [tag.text for tag in self.doc.find_all(name, **kwargs)]
        except Exception:
            logging.error(f"xml parser failed for name={name}.", exc_info=True)
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
