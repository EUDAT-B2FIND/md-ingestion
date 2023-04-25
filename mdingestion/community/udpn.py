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
    PRODUCTIVE = True
    DATE = ''
    DESCRIPTION = """"""
    LOGO = ''
    LINK = ''
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''

