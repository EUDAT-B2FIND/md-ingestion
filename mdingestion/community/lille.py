from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class LilleDatacite(BaseRDG):
    IDENTIFIER = 'lille'
    TITLE = 'Université de Lille'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'univ-lille'               # Set from entrepot univ-lille
    PRODUCTIVE = True
    DATE = '2023-04-28'
    DESCRIPTION = """"""
    LOGO = ''
    LINK = ''
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''