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
        doc.doi = self.find_doi('MD_Identifier.CharacterString')
        doc.creator = self._creator(doc)
        doc.contact = self.find('CI_Contact.linkage')
        doc.discipline = self.discipline(doc, 'Earth System Research')
        doc.publisher = 'World Data Center for Climate (WDCC)'
        doc.version = self.find('MD_DataIdentification.citation.edition')
        doc.format = self.find('MD_Format')
        doc.rights = self._rights(doc)
        doc.funding_reference = self.find('MD_DataIdentification.supplementalInformation.CharacterString')

    def _creator(self,doc):
        selected_creators = []
        try:
            creators = self.reader.parser.doc.MD_DataIdentification.CI_Citation.citedResponsibleParty.find_all('CI_ResponsibleParty')
            for creator in creators:
                try:
                    name = creator.individualName.CharacterString.text
                    codetype = creator.role.CI_RoleCode['codeListValue']
                    if codetype in ['owner', 'originator', 'pointOfContact', 'principalInvestigator', 'author']:
                        selected_creators.append(name)
                except Exception:
                    pass
        except Exception:
            pass
        return selected_creators


    def _rights(self, doc):
        if not doc.rights:
            return 'scientific use: For scientific use only'
        else:
            return doc.rights
