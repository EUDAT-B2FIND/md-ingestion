from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class ULilleDatacite(BaseRDG):
    IDENTIFIER = 'ulille'
    TITLE = 'Université de Lille'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'univ-lille'               # Set from entrepot univ-lille
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-04-28'
    DESCRIPTION = """"""
    LOGO = 'https://entrepot.recherche.data.gouv.fr/logos/125169/Logo.sans.baseline-Horizontal-RVB-Vert-nature.png'
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/univ-lille'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
