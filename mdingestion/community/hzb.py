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
        doc.pid = self.pid_(doc)
#        doc.instrument = self.find('') # <relatedItem relatedItemType="Instrument" relationType="IsCompiledBy">
#              <relatedItemIdentifier relatedItemIdentifierType="DOI">10.5442/NI000001</relatedItemIdentifier>
#              <titles>
#                <title>E2 - Flat-Cone Diffractometer</title>
#              </titles>

    def pid_(self, doc):
        result = []
        pids = self.find('resource.identifier')
        for pid in pids:
            result.append(f'https://hdl.handle.net/{pid}')
        return result
