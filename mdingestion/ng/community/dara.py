from .base import Community
from ..service_types import SchemaType, ServiceType


class BaseDara(Community):
    NAME = 'dara'
    URL = 'https://www.da-ra.de/oaip/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'

    def update(self, doc):
        pass


class DaraRKI(BaseDara):
    IDENTIFIER = 'dara_rki'
    OAI_SET = '98'  # RKI-Bib1 (Robert Koch-Institut), 960 records


class DaraICPSR(BaseDara):
    IDENTIFIER = 'dara_icpsr'
    OAI_SET = '39'  # ICPSR - Interuniversity Consortium for Political and Social Research, 39334


class DaraSRDA(BaseDara):
    IDENTIFIER = 'dara_srda'
    OAI_SET = '128'  # SRDA - Survey Research Data Archive Taiwan, 2680
