from ..core import B2FDoc
from ..parser import XMLParser
from ..parser import JSONParser


class Reader(object):
    DOC_PARSER = None

    def __init__(self):
        self.parser = None

    def read(self, filename, url=None, community=None, mdprefix=None):
        self.parser = self.DOC_PARSER(filename)
        doc = B2FDoc(filename, url, community, mdprefix)
        self._parse(doc)
        self.parse(doc)
        self.update(doc)
        return doc

    @classmethod
    def extension(cls):
        return cls.DOC_PARSER.extension()

    def _parse(self, doc):
        doc.fulltext = self.parser.fulltext

    def parse(self, doc):
        raise NotImplementedError

    def update(self, doc):
        pass


class XMLReader(Reader):
    DOC_PARSER = XMLParser


class JSONReader(Reader):
    DOC_PARSER = JSONParser


class OAIReader(XMLReader):
    def _parse(self, doc):
        doc.oai_set = self.parser.find('setSpec', limit=1)
        doc.oai_identifier = self.parser.find('identifier', limit=1)
        doc.metadata_access = self.metadata_access(doc)
        super()._parse(doc)

    def metadata_access(self, doc):
        if doc.oai_identifier:
            mdaccess = f"{doc.url}?verb=GetRecord&metadataPrefix={doc.mdprefix}&identifier={doc.oai_identifier[0]}"  # noqa
        else:
            mdaccess = None
        return mdaccess
