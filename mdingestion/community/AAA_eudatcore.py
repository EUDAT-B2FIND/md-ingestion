from .base import Repository
from ..service_types import SchemaType, ServiceType


class AAADatacite(Repository):
    IDENTIFIER = 'aaa_eudatcore'
    URL = 'https://darus.uni-stuttgart.de/oai'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = None
    PRODUCTIVE = False
    DATE = '2023-02-08'
    CRON_DAILY = False
    LOGO = "http://b2find.dkrz.de/images/communities/darus_logo.png"
    DESCRIPTION = """ blabla mit nem link https://whatever/ nochmehr blabla """
    REPOSITORY_ID = 'r3d100013118'
    REPOSITORY_NAME = 'FZ-Juelich B2SHARE'    

    def update(self, doc):
        doc.publisher = 'B2Find'
