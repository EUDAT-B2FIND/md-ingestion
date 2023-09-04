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
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-05-15'
    DESCRIPTION = """"""
    LOGO = 'https://entrepot.recherche.data.gouv.fr/logos/148870/5-ecole_ponts20_rvb72petit.jpg'
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/ecoledesponts'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
