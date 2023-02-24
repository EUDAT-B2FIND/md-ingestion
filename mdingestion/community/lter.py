from .base import Repository
from ..service_types import SchemaType, ServiceType


class BaseLter(Repository):
    IDENTIFIER = 'lter'
    NAME = 'lter'
    PRODUCTIVE = False
    DATE = '2023-01-31'
    CRON_DAILY = False
    LOGO = "https://www.envriplus.eu/wp-content/uploads/2015/08/LTER-Europe-logo-short.jpg"
    DESCRIPTION = """The LTER Network was founded in 1980 by the National Science Foundation with the recognition that long-term research could help unravel the principles and processes of ecological science, which frequently involves long-lived species, legacy influences, and rare events. As policymakers and resource managers strive to incorporate reliable science in their decision making, the LTER Network works to generate and share useful and usable information."""
    LINK = "https://lternet.edu/"

    def update(self, doc):
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')


class LterB2ShareCsc(BaseLter):
    IDENTIFIER = 'lter_csc'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '	d952913c-451e-4b5c-817e-d578dc8a4469'  # LTER Set from CSC


class LterB2ShareFzj(BaseLter):
    IDENTIFIER = 'lter_fzj'
    GROUP = 'b2share'
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '	d952913c-451e-4b5c-817e-d578dc8a4469'  # LTER Set from FZJ
