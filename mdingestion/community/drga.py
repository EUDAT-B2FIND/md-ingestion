from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class UnistraDatacite(BaseRDG):
    IDENTIFIER = 'drga'
    TITLE = 'Data Repository Grenoble Alpes'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'univ-grenoble-alpes'               # Set from entrepot univ-grenoble-alpes
    PRODUCTIVE = True
    DATE = ''
    DESCRIPTION = """"""
    LOGO = ''
    LINK = ''
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
