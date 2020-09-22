from ..core import B2FDoc
from ..parser import XMLParser
from ..parser import JSONParser
from ..format import format_url
from ..classify import Classify

import logging


class Reader(object):
    DOC_PARSER = None
    SNIFFER = None

    def __init__(self):
        self.filename = None
        self.parser = None
        self.sniffer = None
        self.errors = dict(invalid_geometry=[])

    def read(self, filename, community=None, url=None, oai_metadata_prefix=None):
        self.filename = filename
        self.parser = self.DOC_PARSER(filename)
        # TODO: handling of oai_metadata_prefix parameter needs to be refactored
        doc = B2FDoc(filename, community, url, oai_metadata_prefix)
        self._parse(doc)
        self.parse(doc)
        if self.SNIFFER:
            sniffer = self.SNIFFER(self.parser)
            sniffer.update(doc)
        return doc

    def _parse(self, doc):
        doc.fulltext = self.parser.fulltext

    def parse(self, doc):
        raise NotImplementedError

    def find(self, name=None, **kwargs):
        return self.parser.find(name=name, **kwargs)

    def find_ok(self, name=None, **kwargs):
        if self.parser.find(name=name, **kwargs):
            return True
        return False

    def find_doi(self, name=None, **kwargs):
        urls = [url for url in self.find(name, **kwargs) if 'doi' in format_url(url)]
        return urls

    def find_pid(self, name=None, **kwargs):
        urls = [url for url in self.find(name, **kwargs) if 'hdl.handle.net' in format_url(url)]
        return urls

    def find_source(self, name=None, **kwargs):
        urls = []
        for url in self.find(name, **kwargs):
            f_url = format_url(url)
            if not f_url:
                continue
            if 'doi' in f_url:
                continue
            if 'hdl.handle.net' in f_url:
                continue
            urls.append(f_url)
        return urls

    def discipline(self, doc, default=None):
        default = default or 'Various'
        classifier = Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        if 'Various' in disc:
            disc = default
        return disc

    def find_geometry(self):
        try:
            geometry = self.geometry()
        except Exception as e:
            logging.warning(f"Could not parse geometry. {e}")
            self.errors['invalid_geometry'].append(self.filename)
            geometry = None
        return geometry

    def geometry(self):
        raise NotImplementedError


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
