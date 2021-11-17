from .service_types import ServiceType


def sniffer(service_type=None):
    if service_type == ServiceType.CSW:
        sniffer = CSWSniffer
    elif service_type == ServiceType.ArcGIS:
        sniffer = ArcGISSniffer
    else:
        sniffer = OAISniffer
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
            mdaccess = f"{doc.url}?verb=GetRecord&metadataPrefix={doc.oai_metadata_prefix}&identifier={doc.oai_identifier}"  # noqa
        else:
            mdaccess = None
        return mdaccess


class CSWSniffer(CatalogSniffer):
    def update(self, doc):
        doc.file_identifier = self.parser.find('fileIdentifier', limit=1)
        doc.metadata_access = self.metadata_access(doc)

    def metadata_access(self, doc):
        if doc.file_identifier:
            # TODO: add schema for iso19139
            mdaccess = f"{doc.url}?service=CSW&version=2.0.2&request=GetRecordById&Id={doc.file_identifier}"
        else:
            mdaccess = None
        return mdaccess


class ArcGISSniffer(CatalogSniffer):
    def update(self, doc):
        doc.metadata_access = self.metadata_access(doc)

    def metadata_access(self, doc):
        identifier = self.parser.find('properties.OBJECTID')[0]
        if identifier:
            mdaccess = f"{doc.url}?objectIds={identifier}&outFields=*&returnGeometry=true&f=geojson"
        else:
            mdaccess = None
        return mdaccess
