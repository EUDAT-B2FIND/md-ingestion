from .base import Repository
from ..service_types import SchemaType, ServiceType


class EcolePontsParisTechDatacite(Repository):
    NAME = 'edpp'
    IDENTIFIER = NAME
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'ecoledesponts'
    PRODUCTIVE = False
