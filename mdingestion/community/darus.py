from .base import Repository
from ..service_types import SchemaType, ServiceType


class DarusDatacite(Repository):
    NAME = 'darus'
    IDENTIFIER = 'darus'
    URL = 'https://darus.uni-stuttgart.de/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2020-09-20'
    LOGO = "http://b2find.dkrz.de/images/communities/darus_logo.png"
    DESCRIPTION = """
    https://www.izus.uni-stuttgart.de/fokus/darus/

    DaRUS - the Data Repository of the University of Stuttgart

    DaRUS is the place to archive, share and publish the research data, scripts and codes of the members and partners of the University of Stuttgart.
    """
