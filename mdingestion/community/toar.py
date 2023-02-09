from .base import Repository
from ..service_types import SchemaType, ServiceType


class Toar(Repository):
    IDENTIFIER = 'toar'
    # URL = 'https://b2share.fz-juelich.de/api/oai2d'
    URL = 'https://b2share-testing.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '26e8202d-7094-412d-be28-7e64bf6ac77f'
    PRODUCTIVE = False
    DATE = '2023-02-09'
    CRON_DAILY = False
    LOGO = "http://b2find.dkrz.de/images/communities/darus_logo.png"
    DESCRIPTION = """ blabla mit nem link https://whatever/ nochmehr blabla """
    REPOSITORY_ID = 'r3d100013118'
    REPOSITORY_NAME = 'FZ-Juelich B2SHARE'


# TODO: add 'Group' B2SHARE
    def update(self, doc):
        doc.publisher = 'Tropospheric Ozone Assessment Report (TOAR)'
        doc.discipline = 'Atmospheric Chemistry'
