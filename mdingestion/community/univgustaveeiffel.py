from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class Data_UnivGustaveEiffelDatacite(BaseRDG):
    IDENTIFIER = 'univgustaveeiffel'
    TITLE = 'Data Université Gustave Eiffel'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'univ-gustave-eiffel'
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-05-15'
    DESCRIPTION = """"""
    LOGO = 'https://www.univ-gustave-eiffel.fr/fileadmin/logo_univ_gustave_eiffel_rvb.svg'
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/univ-gustave-eiffel'
#    REPOSITORY_ID = ''
#    REPOSITORY_NAME = ''
