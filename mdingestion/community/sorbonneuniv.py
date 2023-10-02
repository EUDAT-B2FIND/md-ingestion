from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class SorbonneUnivDatacite(BaseRDG):
    IDENTIFIER = 'sorbonneuniv'
    TITLE = 'Sorbonne Université'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'sorbonne-univ'               # Set from entrepot sorbonne-univ
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-05-10'
    DESCRIPTION = """"""
    LOGO = 'https://fr.wikipedia.org/wiki/Institut_Pasteur#/media/Fichier:Logo_Institut_Pasteur.svg'
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/pasteur'
#    REPOSITORY_ID = ''
#    REPOSITORY_NAME = ''
