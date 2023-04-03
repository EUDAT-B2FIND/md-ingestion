from .base import Repository
from ..service_types import SchemaType, ServiceType


class BaseGesis(Repository):
    NAME = 'gesis'
    TITLE = 'GESIS'
    URL = 'https://dbkapps.gesis.org/dbkoai'
    SERVICE_TYPE = ServiceType.OAI
    REPOSITORY_ID = 're3data:r3d100012496'
    REPOSITORY_NAME = 'GESIS Data Archive'
    DATE = '2023-04-03'
    CRON_DAILY = False
    LOGO = "https://www.gesis.org/fileadmin/_processed_/7/c/csm_GESIS-Logo-kompakt-en_b2d554cd55.jpg"
    DESCRIPTION = """GESIS preserves (mainly quantitative) social research data to make it available to the scientific research community. The data is described in a standardized way, secured for the long term, provided with a permanent identifier (DOI), and can be easily found and reused through browser-optimized catalogs (https://search.gesis.org/)."""
    LINK = 'https://www.gesis.org/en/home', 'https://search.gesis.org'   
   
   
class GesisDbk(BaseGesis):  
    IDENTIFIER = 'gesis_dbk'
    SCHEMA = SchemaType.DDI25
    OAI_METADATA_PREFIX = 'oai_ddi25-en'
    OAI_SET = 'DBK' 
    PRODUCTIVE = False  

    def update(self, doc):
        doc.publication_year = self.find('prodDate')
        doc.temporal_coverage = self.find('collDate')
        doc.places = self.find('geogCover')
        doc.language = 'eng'
        doc.doi = self.find('othId', type='DOI')
        doc.resource_type = 'Dataset'
        doc.discipline = self.discipline(doc, 'Social Sciences')


class GesisSdn(BaseGesis):
    IDENTIFIER = 'gesis_sdn'
    SCHEMA = SchemaType.DDI25
    OAI_METADATA_PREFIX = 'oai_ddi25'
    OAI_SET = 'SDN' 
    PRODUCTIVE = False  

    def update(self, doc):
        doc.publisher = self.find('distrbtr')
        doc.temporal_coverage = self.find('collDate')
        doc.places = self.find('geogCover')
        doc.doi = self.find('othId', type='DOI')
        doc.resource_type = 'Dataset'
        doc.discipline = self.discipline(doc, 'Social Sciences')
