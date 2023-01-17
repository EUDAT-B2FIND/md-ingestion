from .base import Repository
from ..service_types import SchemaType, ServiceType


class ISISDatacite(Repository):
    IDENTIFIER = 'isis'
    URL = 'https://icat-dev.isis.stfc.ac.uk/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = False
#    DATE = '2023-01.17'
#    LOGO = "http://b2find.dkrz.de/images/communities/darus_logo.png"
#    DESCRIPTION = """ some description here """
