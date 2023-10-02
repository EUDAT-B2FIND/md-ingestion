from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class InriaDatacite(BaseRDG):
    IDENTIFIER = 'inria'
    TITLE = 'Inria'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'inria'               # Set from entrepot inria
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-05-10'
    DESCRIPTION = """"""
#    LOGO = 'https://files.inria.fr/dircom/extranet/LOGOS_pack_web.zip' ## use red version - "inr_logo_rouge"
    LINK = 'https://entrepot.recherche.data.gouv.fr/dataverse/inria'
#    REPOSITORY_ID = ''
#    REPOSITORY_NAME = ''
