from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class UniToulouseDatacite(BaseRDG):
    IDENTIFIER = 'utjj'
    TITLE = 'Université Toulouse Jean-Jaurès'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'univ-toulouse-2'               # Set from entrepot univ-toulouse-2
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-05-15'
    DESCRIPTION = """"""
    LOGO = ''
    LINK = ''
#    REPOSITORY_ID = ''
#    REPOSITORY_NAME = ''
