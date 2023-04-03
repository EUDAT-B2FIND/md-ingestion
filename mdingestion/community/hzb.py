from .panoscexpands import BasePanoscExpands
from ..service_types import SchemaType, ServiceType


class HZBDatacite(BasePanoscExpands):
    NAME = 'hzb'
    TITLE = 'HZB'
    URL = 'https://data.helmholtz-berlin.de/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    PRODUCTIVE = False
    DATE = ''
    CRON_DAILY = False
    LOGO = "https://www.helmholtz-berlin.de/media/design/logo/hzb-logo.svg"
    DESCRIPTION = """Helmholtz-Zentrum Berlin für Materialien und Energie (HZB) strives to achieve a climate neutral society through science and innovation. Scientists are developing and optimising efficient and cost-effective materials amongst others for solar cells, batteries and catalysts. Therefore, we run state-of-the art labs and BESSY II light source, which delivers intensely bright light, soft X-rays in particular. Researchers are using this special light to study the structure and function of energy and quantum materials. BESSY II has an annual average of 2700 user visits from 28 countries. HZB scientists are also conducting research on new concepts for accelerator-based light sources."""
    REPOSITORY_ID = 'https://ror.org/02aj13c28'
    REPOSITORY_NAME = 'Helmholtz-Zentrum Berlin für Materialien und Energie'


class HZBpub(HZBDatacite):
    IDENTIFIER = 'hzb_pub'
    OAI_SET = 'pub'


class HZBinv(HZBDatacite):
    IDENTIFIER = 'hzb_inv'
    OAI_SET = 'raw_inv'

    def update(self, doc):
        doc.pid = self.pid_(doc)
        doc.instrument = self.instrument(doc)
        doc.creator = self.creator(doc)

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

    def creator(self, doc):
        new_creator = [c for c in doc.creator if c != ':unav']
        return new_creator
