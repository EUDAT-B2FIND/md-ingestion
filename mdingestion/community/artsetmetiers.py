from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class ArtsetmetiersDatacite(BaseRDG):
    IDENTIFIER = 'artsetmetiers'
    TITLE = 'Arts et Métiers Institute of Technology'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'artsetmetiers'
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-05-15'
    DESCRIPTION = """"""
    LOGO = 'https://entrepot.recherche.data.gouv.fr/logos/152473/logo-trans-322x84.png'
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/artsetmetiers'
#    REPOSITORY_ID = ''
#    REPOSITORY_NAME = ''
