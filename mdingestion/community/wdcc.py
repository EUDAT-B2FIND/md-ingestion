from .base import Repository
from ..service_types import SchemaType, ServiceType


class WDCCIso(Repository):
    IDENTIFIER = 'wdcc'
    TITLE = 'WDCC - World Data Centre for Climate'
    URL = 'https://dmoai.cloud.dkrz.de/oai/provider'
    SCHEMA = SchemaType.ISO19139
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'iso19115'
    PRODUCTIVE = False
    CRON_DAILY = False
    DATE = '2023-01-10'
    LOGO = "http://b2find.dkrz.de/images/communities/wdcc_logo.png"
    DESCRIPTION = """
    ISO Template with ENES
    """
    REPOSITORY_ID = 're3data:r3d100010299'
    REPOSITORY_NAME = 'World Data Center for Climate'

    def update(self, doc):
        doc.doi = self.find_doi('identificationInfo.MD_DataIdentification.citation.identifier.MD_Identifier')
        doc.contact = self.find('CI_ResponsibleParty.organisationName')
        doc.contributor = 'World Data Center for Climate (WDCC)'
        doc.discipline = self.discipline(doc, 'Earth System Research')
