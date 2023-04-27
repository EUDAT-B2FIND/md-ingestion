from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class EdppDatacite(BaseRDG):
    IDENTIFIER = 'edpp'
    TITLE = 'École des Ponts-ParisTech'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'ecoledesponts'               # Set from entrepot edpp
    PRODUCTIVE = True
    DATE = ''
    DESCRIPTION = """"""
    LOGO = ''
    LINK = ''
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
