from .base import Repository
from ..service_types import SchemaType, ServiceType


class HZBDatacite(Repository):
    NAME = 'hzb'
    URL = 'https://data.helmholtz-berlin.de/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
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
        doc.instrument = self.instrument(doc)

    def pid_(self, doc):
        result = []
        pids = self.find('resource.identifier')
        for pid in pids:
            result.append(f'https://hdl.handle.net/{pid}')
        return result

    def instrument(self, doc):
        result = []
        insts = self.reader.parser.doc.find_all('relatedItem', relatedItemType="Instrument")
        for inst in insts:
            title = inst.titles.title.text
            ident = inst.relatedItemIdentifier.text
            ident_type = inst.relatedItemIdentifier['relatedItemIdentifierType']
            if ident_type == 'DOI':
                result.append(f'{title}, https://doi.org/{ident}')
            elif ident_type == 'PID':
                result.append(f'{title}, https://hdl.handle.net/{ident}')
        return result
