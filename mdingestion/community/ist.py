from .base import Community
from ..service_types import SchemaType, ServiceType


class IstDublinCore(Community):
    NAME = 'ist'
    IDENTIFIER = 'ist'
    URL = 'https://research-explorer.app.ist.ac.at/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = 'research_dataFtxt'
    PRODUCTIVE = True

    def update(self, doc):
        doc.doi = self.doi()
        doc.source = self.source()
        doc.discipline = self.discipline(doc, 'Life Sciences, Natural Sciences, Engineering Sciences')
        doc.contact = 'repository.manager@ist.ac.at'

    def doi(self):
        doi_info = 'info:eu-repo/semantics/altIdentifier/doi/'
        dois = [id for id in self.find('metadata.relation') if id.startswith(doi_info)]
        if dois:
            url = f"https://doi.org/{dois[0].split(doi_info)[1]}"
        else:
            url = ''
        return url

    def source(self):
        urls = [url for url in self.find('metadata.identifier') if 'app.ist.ac.at/record' in url]
        return urls
