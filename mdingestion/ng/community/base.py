from ..reader import build_reader, SchemaType
from ..sniffer import ServiceType


class Community(object):
    NAME = None
    IDENTIFIER = None
    URL = None
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI

    def __init__(self):
        self.reader = build_reader(self.SCHEMA, self.SERVICE_TYPE)

    def read(self, filename):
        doc = self.reader.read(filename, community=self.NAME, url=self.URL)
        self.update(doc)
        return doc

    def find(self, name=None, **kwargs):
        return self.reader.find(name=name, **kwargs)

    def find_ok(self, name=None, **kwargs):
        return self.reader.find_ok(name=name, **kwargs)

    def find_doi(self, name=None, **kwargs):
        return self.reader.find_doi(name=None, **kwargs)

    def find_pid(self, name=None, **kwargs):
        return self.reader.find_pid(name=None, **kwargs)

    def find_source(self, name=None, **kwargs):
        return self.reader.find_source(name=None, **kwargs)

    def discipline(self, doc, default=None):
        return self.reader.discipline(doc, default)

    @property
    def errors(self):
        return self.reader.errors

    def update(self, doc):
        raise NotImplementedError
