from .base import Repository
from ..service_types import SchemaType, ServiceType


class BaseEudat(Repository):
    IDENTIFIER = 'eudat'
    NAME = 'EUDAT'
    PRODUCTIVE = True
    DATE = '2023-01-23'

    def update(self, doc):
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')


class EudatCsc(BaseEudat):
    IDENTIFIER = 'eudat_csc'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = 'e9b9792e-79fb-4b07-b6b4-b9c2bd06d095'  # EUDAT Set from CSC
    EPOSITORY_ID = 're3data:r3d100011394'
    REPOSITORY_NAME = 'B2SHARE'


class EudatFzj(BaseEudat):
    IDENTIFIER = 'eudat_fzj'
    GROUP = 'b2share'
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = 'e9b9792e-79fb-4b07-b6b4-b9c2bd06d095'  # EUDAT Set from FZJ
    EPOSITORY_ID = 're3data:r3d100013118'
    REPOSITORY_NAME = 'B2SHARE Server Forschungszentrum Jülich'
