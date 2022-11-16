from .base import Community
from ..service_types import SchemaType, ServiceType


class UniversitedeStrasbourgDatacite(Community):
    NAME = 'uds'
    IDENTIFIER = NAME
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = ''
    PRODUCTIVE = False
#    Date = 