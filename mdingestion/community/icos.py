from .base import Repository
from ..service_types import SchemaType, ServiceType


class IcosDatacite(Repository):
    IDENTIFIER = 'icos'
    URL = 'https://oai.datacite.org/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'datacite'
    OAI_SET = 'SND.ICOS'
    PRODUCTIVE = False
    DATE = '2023-01-31'
    CRON_DAILY = False
    LOGO = "https://www.icos-cp.eu/sites/default/files/ICOS-logo.svg"
    LINK = ''
    DESCRIPTION = """ICOS Carbon Portal is the data portal of the Integrated Carbon Observation System. It provides observational data from the state of the carbon cycle in Europe and the world. The Carbon Portal is the data center of the ICOS infrastructure. ICOS will collect greenhouse gas concentration and fluxes observations from three separate networks, all these observations are carried out to support research to help us understand how the Earth’s greenhouse gas balance works, because there are still many and large uncertainties!"""
    LINK = 'https://www.icos-cp.eu/'
    REPOSITORY_ID = 're3data:r3d100012203'
    REPOSITORY_NAME = 'ICOS Carbon Portal'
