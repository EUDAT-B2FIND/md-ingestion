from .base import Repository
from ..service_types import SchemaType, ServiceType


class AAADatacite(Repository):
    IDENTIFIER = 'aaa_datacite'
    URL = 'https://darus.uni-stuttgart.de/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = False
    DATE = '2023-01-10'
    LOGO = "http://b2find.dkrz.de/images/communities/darus_logo.png"
    DESCRIPTION = """
    Datacite Template with Darus

    https://www.izus.uni-stuttgart.de/fokus/darus/

    DaRUS - the Data Repository of the University of Stuttgart

    DaRUS is the place to archive, share and publish the research data, scripts and codes of the members and partners of the University of Stuttgart.
    """

    def update(self, doc):
        doc.publisher = 'B2Find'