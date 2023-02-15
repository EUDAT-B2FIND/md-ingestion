from .base import Repository
from ..service_types import SchemaType, ServiceType


class EdmondDublincore(Repository):
    IDENTIFIER = 'edmond'
    URL = 'https://oai.datacite.org/oai'
    SCHEMA = SchemaType.DataCite
    # SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    # OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = 'TIB.MPDL'
    PRODUCTIVE = False
    DATE = '2023-01-10'
    CRON_DAILY = False
    LOGO = "https://edmond.mpdl.mpg.de/logos/navbar/logo_for_bright.png;jsessionid=5358848a5a7c5bebb3eb15bec784"
    DESCRIPTION = """Edmond is a research data repository for Max Planck researchers. It is the place to store completed datasets of research data with open access. Edmond serves the publication of research data from all disciplines and offers scientists the ability to create citable research objects. https://edmond.mpdl.mpg.de/"""
    REPOSITORY_ID = 'http://doi.org/10.17616/R3N33V'
    REPOSITORY_NAME = 'EDMOND'
