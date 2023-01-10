from .base import Repository
from ..service_types import SchemaType, ServiceType


class AAAGroup(Repository):
    GROUP = 'aaa_group'
    GROUP_TITLE = 'AAA Group Template'
    PRODUCTIVE = False
    DATE = '2023-01-10'
    DESCRIPTION = 'Group Template'
    LOGO = ''
    URL = 'https://www.da-ra.de/oaip/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'

    def update(self, doc):
        pass


class AAAGroupOne(AAAGroup):
    IDENTIFIER = 'aaa_group_one'
    TITLE = 'AAA Group One'
    OAI_SET = '1'  # GESIS Data Archive, 7783 records

    def update(self, doc):
        doc.publisher = 'B2Find'


class AAAGroupTwo(AAAGroup):
    IDENTIFIER = 'aaa_group_two'
    TITLE = 'AAA Group Two'
    OAI_SET = '10'  # RKI Robert Koch-Institut, 15 records

    def update(self, doc):
        doc.publisher = 'B2Find'
