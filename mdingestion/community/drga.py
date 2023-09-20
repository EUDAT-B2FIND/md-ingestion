from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class DRGADatacite(BaseRDG):
    IDENTIFIER = 'drga'
    TITLE = 'Data Repository Grenoble Alpes'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'univ-grenoble-alpes'               # Set from entrepot univ-grenoble-alpes
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-05-15'
    DESCRIPTION = """"""
    LOGO = 'https://entrepot.recherche.data.gouv.fr/logos/125168/logo_UGA_couleur_cmjn.jpg'
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/univ-grenoble-alpes'
#    REPOSITORY_ID = ''
#    REPOSITORY_NAME = ''
