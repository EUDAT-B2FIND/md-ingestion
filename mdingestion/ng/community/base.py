from ..reader import build_reader
from ..sniffer import build_sniffer


class Community(object):
    def __init__(self, name, url=None, mdprefix=None, schema=None, service_type=None):
        self.name = name
        self.url = url
        self.mdprefix = mdprefix
        self.service_type = service_type
        self.reader = build_reader(schema)
        self._sniffer = None

    def read(self, filename):
        doc = self.reader.read(
            filename,
            url=self.url,
            community=self.name,
            mdprefix=self.mdprefix)
        self.sniffer.update(doc)
        self.update(doc)
        return doc

    @property
    def sniffer(self):
        if not self._sniffer:
            self._sniffer = build_sniffer(self.parser, self.service_type)
        return self._sniffer

    @property
    def parser(self):
        return self.reader.parser

    def update(self, doc):
        raise NotImplementedError
