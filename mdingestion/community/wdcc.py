from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..enhance import count_citations

lookup_citations = count_citations()


import requests

def lookup_raids(doi):
    url = 'https://api.test.datacite.org/dois'
    params = {
        'provider-id': 'ardcx',
        'query': f'relatedIdentifiers.relatedIdentifier:"{doi}"'
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        result = response.json()
        # Extract only the 'id' values from the 'data' key
        ids = [item['id'] for item in result.get('data', [])]
        return ids
    else:
        return []


class WDCCIso(Repository):
    IDENTIFIER = 'wdcc'
    TITLE = 'WDCC - World Data Centre for Climate'
    URL = 'https://dmoai.cloud.dkrz.de/oai/provider'
    SCHEMA = SchemaType.ISO19139
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'iso19115'
    PRODUCTIVE = True
    CRON_DAILY = False
    DATE = '2022-01-10'
    LOGO = "http://b2find.dkrz.de/images/communities/wdcc_logo.png"
    REPOSITORY_ID = 're3data:r3d100010299'
    REPOSITORY_NAME = 'World Data Center for Climate'

    def update(self, doc):
        doc.doi = self.find_doi('MD_Identifier.CharacterString')
        doc.related_identifier = self.related_identifier_raid(doc)
        doc.contact = self.find('CI_Contact.linkage')
        doc.discipline = self.discipline(doc, 'Earth System Research')
        doc.publisher = 'World Data Center for Climate (WDCC)'
        doc.version = self.find('MD_DataIdentification.citation.edition')
        doc.format = self.find('MD_Format')
        doc.resource_type = self.find('MD_ScopeCode')
        doc.size = self._size(doc)
        doc.rights = self._rights(doc)
        doc.funding_reference = self.find('MD_DataIdentification.supplementalInformation.CharacterString')
        doc.citations = self._citations(doc)

    def _size(self,doc):
        sizes = doc.size
        return [f'{s} MB' for s in sizes]

    def _rights(self, doc):
        if not doc.rights:
            return 'scientific use: For scientific use only'
        else:
            return doc.rights


    def _citations(self,doc):
        doi = doc.doi
        doi = doi.lower()
        if doi in lookup_citations:
            citations = lookup_citations[doi]
        else:
            citations = 0
        return citations

    def related_identifier_raid(self,doc):
        relids = []
        raids = lookup_raids(doc.doi)
        for raid in raids:
            relid = f"{raid}|DOI|hasRAiD"
            relids.append(relid)
        return relids
