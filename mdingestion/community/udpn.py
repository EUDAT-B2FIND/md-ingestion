from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class UniversiteParisNanterreDatacite(BaseRDG):
    IDENTIFIER = 'udpn'
    TITLE = 'Université de Paris-Nanterre'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'univ-paris-nanterre'               # Set from entrepot univ-paris-nanterre
    PRODUCTIVE = False
    DATE = '12-05-2023'
    DESCRIPTION = """"""
    LOGO = 'https://entrepot.recherche.data.gouv.fr/logos/125171/logo_Paris_Nanterre_couleur_CMJN.jpg'
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/univ-paris-nanterre'
#    REPOSITORY_ID = ''
#    REPOSITORY_NAME = ''
