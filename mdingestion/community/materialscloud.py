from .base import Repository
from ..service_types import SchemaType, ServiceType


class MaterialscloudDublinCore(Repository):
    IDENTIFIER = 'materialscloud'
    URL = 'https://archive.materialscloud.org/xml'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
        doc.doi = self.doi(doc)
        doc.source = self.source(doc)
        # doc.open_access = True
        doc.discipline = 'Materials Science and Engineering'
        doc.contact = 'archive@materialscloud.org'

    def doi(self, doc):
        dois = [id for id in self.find('metadata.identifier') if id.startswith('doi:')]
        if dois:
            url = f"https://doi.org/{dois[0][4:]}"
        else:
            url = ''
        return url

    def source(self, doc):
        urls = [url for url in self.find('metadata.identifier') if 'archive.materialscloud.org' in url]
        return urls
