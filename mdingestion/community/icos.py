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
    DESCRIPTION = """ Integrated Carbon Observation System (ICOS https://www.icos-cp.eu/data-services/about-data-portal) is a pan-European research infrastructure which provides harmonised and high-precision scientific data on carbon cycle and greenhouse gas budget and perturbations.
    """
