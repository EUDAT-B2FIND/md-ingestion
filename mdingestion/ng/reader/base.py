from ..core import B2FDoc
from ..parser import XMLParser
from ..parser import JSONParser


class Reader(object):
    DOC_PARSER = None
    SNIFFER = None

    def __init__(self):
        self.parser = None

    def read(self, filename, url=None, community=None, mdprefix=None):
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


class CatalogSniffer(object):
    def __init__(self, parser):
        self.parser = parser

    def update(self, doc):
        raise NotImplementedError


class OAISniffer(CatalogSniffer):
    def update(self, doc):
        doc.oai_set = self.parser.find('setSpec', limit=1)
        doc.oai_identifier = self.parser.find('identifier', limit=1)
        doc.metadata_access = self.metadata_access(doc)

    def metadata_access(self, doc):
        if doc.oai_identifier:
            mdaccess = f"{doc.url}?verb=GetRecord&metadataPrefix={doc.mdprefix}&identifier={doc.oai_identifier}"
        else:
            mdaccess = None
        return mdaccess


class CSWSniffer(CatalogSniffer):
    def update(self, doc):
        doc.file_identifier = self.parser.find('fileIdentifier', limit=1)
        doc.metadata_access = self.metadata_access(doc)

    def metadata_access(self, doc):
        if doc.file_identifier:
            mdaccess = f"{doc.url}?service=CSW&version=2.0.2&request=GetRecordById&Id={doc.file_identifier}"
        else:
            mdaccess = None
        return mdaccess
