from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class Data_UMontpellierDatacite(BaseRDG):
    IDENTIFIER = 'umontpellier'
    TITLE = 'Data_UMontpellier'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'umontpellier'
    PRODUCTIVE = True
    DATE = '2023-05-15'
    DESCRIPTION = """"""
    LOGO = 'https://www.umontpellier.fr/wp-content/uploads/2022/10/logo_um_2022_rouge_rvb.svg'
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/umontpellier'
#    REPOSITORY_ID = ''
#    REPOSITORY_NAME = ''
