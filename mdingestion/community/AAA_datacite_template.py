from .base import Repository
from ..service_types import SchemaType, ServiceType


class AAADatacite(Repository):
    IDENTIFIER = 'aaa_datacite'
    TITLE = 'we need this title'
    URL = 'https://darus.uni-stuttgart.de/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = False
    DATE = '2023-01-10'
    REPOSITORY_ID = 're3data:r3d100013171'
    REPOSITORY_NAME = 'DaRUS'
    CRON_DAILY = False
    LOGO = "http://b2find.dkrz.de/images/communities/darus_logo.png"
    LINK = 'https://www.izus.uni-stuttgart.de/fokus/darus/'
    DESCRIPTION = """
    Datacite Template with Darus
    DaRUS - the Data Repository of the University of Stuttgart. DaRUS is the place to archive, share and publish the research data, scripts and codes of the members and partners of the University of Stuttgart.
    """

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        if not doc.discipline:
            doc.discipline = 'whatever'
