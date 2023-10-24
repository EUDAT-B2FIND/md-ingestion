from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class INRAEDatacite(BaseRDG):
    IDENTIFIER = 'inrae'
    TITLE = 'INRAE'
#   URL = 'https://data.inrae.fr/oai'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
#    OAI_SET = 'INRAE'
    OAI_SET = 'INRAE_PNDB'
    PRODUCTIVE = True
    DATE = '2023-03-31'
    REPOSITORY_ID = 're3data:r3d100012673'
    REPOSITORY_NAME = 'Data INRAE'

    def update(self, doc):
        super().update(doc)  # moved INRAE discipline mapping to Project level (Recherche Data Gouv)
        handle = format_value(self.find('resource.identifier', identifierType="Handle"), one=True)
        if handle:
            urls = self.reader.pid()
            urls.append(f'http://hdl.handle.net/{handle}')
            doc.pid = urls
        if not doc.publisher:
            doc.publisher = 'INRAE'
