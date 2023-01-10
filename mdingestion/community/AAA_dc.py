from .base import Repository
from ..service_types import SchemaType, ServiceType


class AAADC(Repository):
    IDENTIFIER = 'aaa_dc'
    URL = 'http://www.seanoe.org/oai/OAIHandler'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = False
    DATE = '2023-01-10'
    LOGO = "http://b2find.dkrz.de/images/communities/seanoe_logo.png"
    DESCRIPTION = """
    DublinCore Template with SEANOE
    """

    def update(self, doc):
        doc.publisher = 'B2Find'
