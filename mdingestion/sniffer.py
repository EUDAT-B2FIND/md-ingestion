from .service_types import ServiceType, SchemaType


def sniffer(service_type=None):
    if service_type == ServiceType.CSW:
        sniffer = CSWSniffer
    elif service_type == ServiceType.ArcGIS:
        sniffer = ArcGISSniffer
    elif service_type == ServiceType.BC:
        sniffer = BlueCloudSniffer
    elif service_type == ServiceType.OAI_IVOA:
        sniffer = OAISniffer
    elif service_type == ServiceType.Dataverse:
        sniffer = DataverseSniffer
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
            url = doc.url.strip()
            mdaccess = f"{url}?verb=GetRecord&metadataPrefix={doc.oai_metadata_prefix}&identifier={doc.oai_identifier}"  # noqa
        else:
            mdaccess = None
        return mdaccess


class CSWSniffer(CatalogSniffer):
    def update(self, doc):
        doc.file_identifier = self.parser.find('fileIdentifier', limit=1)
        doc.metadata_access = self.metadata_access(doc)

    def metadata_access(self, doc):
        if doc.file_identifier:
            if doc.schema == SchemaType.ISO19139:
                mdaccess = f"{doc.url}?service=CSW&version=2.0.2&request=GetRecordById&Id={doc.file_identifier}&outputSchema=http://www.isotc211.org/2005/gmd"
            else:
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


class BlueCloudSniffer(CatalogSniffer):
    def update(self, doc):
        doc.metadata_access = self.metadata_access(doc)

    def metadata_access(self, doc):
        identifier = self.parser.find('Identifier')[0]
        if identifier:
            mdaccess = f"{doc.url}/{identifier}"
        else:
            mdaccess = None
        return mdaccess

class DataverseSniffer(CatalogSniffer):
    def update(self, doc):
        doc.metadata_access = self.metadata_access(doc)

    def metadata_access(self, doc):
        identifier = self.parser.find('global_id')[0]
        if identifier:
            # https://edmond.mpdl.mpg.de/api/search?q=global_id:"doi:10.17617/3.EMRZGH"&type=dataset&per_page=1
            mdaccess = f"{doc.url}/api/datasets/export?exporter=dataverse_json&persistentId={identifier}"
        else:
            mdaccess = None
        return mdaccess
