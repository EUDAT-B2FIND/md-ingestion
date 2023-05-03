from .base import Repository
from ..service_types import SchemaType, ServiceType


class DatadoiDublincore(Repository):
    IDENTIFIER = 'datadoi'
    TITLE = 'DataDOI'
    URL = 'https://datadoi.ee/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = False
    DATE = '2023-0503'
    CRON_DAILY = False
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    LOGO = ""
    DESCRIPTION = """
    DublinCore Template with SEANOE
    """

    def update(self, doc):
        doc.publisher = 'B2Find'
