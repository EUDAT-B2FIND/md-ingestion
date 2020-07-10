from enum import Enum


class ServiceType(Enum):
    OAI = 0
    CSW = 1


def build_sniffer(parser, service_type=None):
    if service_type == ServiceType.CSW:
        sniffer = CSWSniffer(parser)
    else:
        sniffer = OAISniffer(parser)
    return sniffer


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
