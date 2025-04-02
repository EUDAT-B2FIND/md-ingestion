from .base import Repository
from ..service_types import SchemaType, ServiceType


class DARADatacite(Repository):
    IDENTIFIER = 'dara2'
    TITLE = 'DA|RA'
    URL = 'https://api.datacite.org/'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.DataCite
    FILTER = "daraco"
    PRODUCTIVE = True
    DATE = '2025-04-02'
    CRON_DAILY = False
    REPOSITORY_ID = ''
    REPOSITORY_NAME = 'DA|RA'
    LOGO = ""
    DESCRIPTION = ""
