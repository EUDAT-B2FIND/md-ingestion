from .base import Repository
from ..service_types import SchemaType, ServiceType


class HZBDatacite(Repository):
    NAME = 'hzb'
    URL = 'https://data.helmholtz-berlin.de/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
#    OAI_SET = 'pub'
    PRODUCTIVE = False
    DATE = ''
    CRON_DAILY = False
    LOGO = "http://b2find.dkrz.de/images/communities/hzb_logo.png"
    DESCRIPTION = ""
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''


class HZBpub(HZBDatacite):
    IDENTIFIER = 'hzb_pub'
    OAI_SET = 'pub'


class HZBinv(HZBDatacite):
    IDENTIFIER = 'hzb_inv'
    OAI_SET = 'raw_inv'

    def update(self, doc):
#        doc.identifier = # <identifier identifierType="Handle">21.11151/2b22-bq4v</identifier> klickbar machen
        doc.instrument = self.find('') # <relatedItem relatedItemType="Instrument" relationType="IsCompiledBy">
              <relatedItemIdentifier relatedItemIdentifierType="DOI">10.5442/NI000001</relatedItemIdentifier>
              <titles>
                <title>E2 - Flat-Cone Diffractometer</title>
              </titles>
