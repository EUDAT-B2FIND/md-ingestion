from .base import Repository
from ..service_types import SchemaType, ServiceType


class GesisDatacite(Repository):
    IDENTIFIER = 'gesis'
    TITLE = 'GESIS'
    URL = 'https://dbkapps.gesis.org/dbkoai'
    SCHEMA = SchemaType.DDI25
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_ddi25'
    OAI_SET = 'DKB' 
    PRODUCTIVE = False
    DATE = '2023-04-03'
    REPOSITORY_ID = 're3data:r3d100012496'
    REPOSITORY_NAME = 'GESIS Data Archive'
    CRON_DAILY = False
    LOGO = "https://www.gesis.org/fileadmin/_processed_/7/c/csm_GESIS-Logo-kompakt-en_b2d554cd55.jpg"
    DESCRIPTION = """GESIS preserves (mainly quantitative) social research data to make it available to the scientific research community. The data is described in a standardized way, secured for the long term, provided with a permanent identifier (DOI), and can be easily found and reused through browser-optimized catalogs (https://search.gesis.org/)."""
    LINK = 'https://www.gesis.org/en/home', 'https://search.gesis.org'

    #def update(self, doc):
        #doc.publisher = 'B2Find'
