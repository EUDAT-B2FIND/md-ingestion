from .base import Repository
from ..service_types import SchemaType, ServiceType


class AAADC(Repository):
    IDENTIFIER = 'aaa_dc'
    TITLE = 'we need this title'
    URL = 'http://www.seanoe.org/oai/OAIHandler'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = False
    DATE = '2023-01-10'
    CRON_DAILY = False
    REPOSITORY_ID = 're3data:r3d100013171'
    REPOSITORY_NAME = 'DaRUS'
    LOGO = "http://b2find.dkrz.de/images/communities/seanoe_logo.png"
    DESCRIPTION = """
    DublinCore Template with SEANOE
    """
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''

    def update(self, doc):
        doc.publisher = 'B2Find'
