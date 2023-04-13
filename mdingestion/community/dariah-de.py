from .base import Repository
from ..service_types import SchemaType, ServiceType


class DariahDE(Repository):
    IDENTIFIER = 'dariah-de'
    URL = 'https://repository.de.dariah.eu/1.0/oaipmh/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = False
#    DATE = '2023-01-10'
    CRON_DAILY = False
#    LOGO = ""
    DESCRIPTION = """

    https://repository.de.dariah.eu/search/

    DARIAH-DE

    The DARIAH-DE Repository is a central component of the DARIAH-DE Research Data Federation Architecture. It aggregates various services and applications which makes its use very convenient. The repository allows for storing research data sustainably and securely, providing them with metadata, and finding them through the Repository Search as well as through the DARIAH-DE Generic Search. Read more about sustainability, FAIR and Open Access in the Mission Statement of the DARIAH-DE Repository.
    """
    REPOSITORY_ID = 're3data:r3d100011345'
    REPOSITORY_NAME = 'DARIAH-DE'

    def update(self, doc):
        doc.discipline = doc.discipline = self.discipline(doc, 'Humanities')
