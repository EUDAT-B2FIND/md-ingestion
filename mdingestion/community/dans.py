from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class BaseDans(Repository):
    GROUP = 'dans'
    GROUP_TITLE = 'DANS'
    PRODUCTIVE = True
    DATE = '2024-08-28'
    DESCRIPTION = 'DANS is the Dutch national centre of expertise and repository for research data. We help researchers make their data available for reuse. This allows researchers to use the data for new research and makes published research verifiable and reproducible. With more than 300,000 datasets and a staff of 60, DANS is one of the leading repositories in Europe.'
    LOGO = 'http://b2find.dkrz.de/images/communities/danseasy_logo.png'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'

    # Data Station Archaeology
    # Data Station SSH
    # Data Station LifeSciences
    # Data Station Physical and Technical Sciences
    # DataverseNL
