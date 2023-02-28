from .base import Repository
from ..service_types import SchemaType, ServiceType


class HZBDatacite(Repository):
    IDENTIFIER = 'hzb'
    URL = 'https://data.helmholtz-berlin.de/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'pub'
    PRODUCTIVE = False
    DATE = ''
    CRON_DAILY = False
    LOGO = "http://b2find.dkrz.de/images/communities/hzb_logo.png"
    DESCRIPTION = ""
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
