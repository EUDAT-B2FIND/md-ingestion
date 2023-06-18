from .base import Repository
from ..service_types import SchemaType, ServiceType


class HelmholtzEudatcore(Repository):
    IDENTIFIER = 'helmholtz'
    TITLE = 'Helmholtz'
    GROUP = 'b2share'
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '5bbb7b0d-51ed-410a-865b-69d6334a3f0e'  # Helmholtz from FZJ
    PRODUCTIVE = False
    DATE = '2023-06-17'
    EPOSITORY_ID = 're3data:r3d100013118'
    REPOSITORY_NAME = 'B2SHARE Server Forschungszentrum Jülich'
    LINK = ''
    DESCRIPTION = """Research data from HELMHOLTZ association, stored at B2SHARE instance at FZ Jülich."""
