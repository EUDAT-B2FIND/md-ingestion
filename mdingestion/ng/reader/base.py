from enum import Enum

from ..core import B2FDoc
from ..parser import XMLParser
from ..parser import JSONParser


class SchemaType(Enum):
    DublinCore = 0
    DataCite = 1
    ISO19139 = 2
    JSON = 100


class Reader(object):
    DOC_PARSER = None
    SNIFFER = None

    def __init__(self):
        self.parser = None

    def read(self, filename, url=None, community=None, mdprefix=None):
        self.community = community
        self.parser = self.DOC_PARSER(filename)
        doc = B2FDoc(filename, url, community, mdprefix)
        self._parse(doc)
        self.parse(doc)
        if self.SNIFFER:
            sniffer = self.SNIFFER(self.parser)
            sniffer.update(doc)
        self.update(doc)
        return doc

    def _parse(self, doc):
        doc.fulltext = self.parser.fulltext

    def parse(self, doc):
        raise NotImplementedError

    def update(self, doc):
        pass

    def find(self, name=None, **kwargs):
        return self.parser.find(name=name, **kwargs)

    def find_ok(self, name=None, **kwargs):
        if self.parser.find(name=name, **kwargs):
            return True
        return False

    def find_doi(self, name=None, **kwargs):
        urls = [url for url in self.parser.find(name, **kwargs) if 'doi.org/' in url]
        return urls

    def find_pid(self, name=None, **kwargs):
        urls = [url for url in self.parser.find(name, **kwargs) if 'hdl.handle.net/' in url]
        return urls


class XMLReader(Reader):
    DOC_PARSER = XMLParser

    @classmethod
    def extension(cls):
        return '.xml'


class JSONReader(Reader):
    DOC_PARSER = JSONParser

    @classmethod
    def extension(cls):
        return '.json'
