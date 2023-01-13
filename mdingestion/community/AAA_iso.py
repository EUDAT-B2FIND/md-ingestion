from .base import Repository
from ..service_types import SchemaType, ServiceType


class AAAIso(Repository):
    IDENTIFIER = 'aaa_iso'
    URL = 'http://c3grid1.dkrz.de:8080/oai/provider'
    SCHEMA = SchemaType.ISO19139
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'iso'
    OAI_SET = 'iso-old-doi'
    PRODUCTIVE = False
    DATE = '2023-01-10'
    LOGO = "http://b2find.dkrz.de/images/communities/wdcc_logo.png"
    DESCRIPTION = """
    ISO Template with ENES
    """

    def update(self, doc):
        doc.publisher = 'B2Find'
