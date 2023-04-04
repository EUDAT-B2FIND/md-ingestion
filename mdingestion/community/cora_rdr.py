from .base import Repository
from ..service_types import SchemaType, ServiceType


class CORADC(Repository):
    IDENTIFIER = 'cora'
    TITLE = 'CORA.RDR'
    URL = 'https://dataverse.csuc.cat/oai'
#    SCHEMA = SchemaType.DublinCore
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
#    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'OPENAIRE'
    PRODUCTIVE = False
    DATE = '2023-01-10'
    CRON_DAILY = False
    REPOSITORY_ID = 're3data:r3d100013559'
    REPOSITORY_NAME = 'CORA. Repositori de Dades de Recerca'
    LOGO = "http://b2find.dkrz.de/images/communities/cora-rdr.png"
    DESCRIPTION = """
    The CORA.Repositori de Dades de Recerca (CORA.RDR) is a repository of open, curated and FAIR data that covers all academic disciplines. CORA.RDR is a shared service provided by participating Catalan institutions (Universities and CERCA Research Centers). The repository is managed by the CSUC and technical infrastructure is based on Dataverse.
    """

#    def update(self, doc):
#        doc.publisher = 'B2Find'
