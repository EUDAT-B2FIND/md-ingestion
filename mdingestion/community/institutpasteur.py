from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class InstitutPasteurDatacite(BaseRDG):
    IDENTIFIER = 'institutpasteur'
    TITLE = 'Institut Pasteur'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'pasteur'               # Set from entrepot pasteur
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-05-10'
    DESCRIPTION = """"""
    LOGO = 'https://fr.wikipedia.org/wiki/Institut_Pasteur#/media/Fichier:Logo_Institut_Pasteur.svg'
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/pasteur'
#    REPOSITORY_ID = ''
#    REPOSITORY_NAME = ''
