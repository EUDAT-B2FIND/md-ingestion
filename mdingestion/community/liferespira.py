from .base import Repository
from ..service_types import SchemaType, ServiceType


class LiferespiraEudatcore(Repository):
    IDENTIFIER = 'life-respira'
    TITLE = 'LIFE+Respira'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = 'e1800bc8-780e-4617-a7b6-2312cb6190c4'  # LIFE+Respira Set from CSC
    PRODUCTIVE = False
    DATE = '2023-01-23'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False
    LOGO = ""
    DESCRIPTION = """Life+Respira is a project aimed at showing that through the use of new technologies, along with urban planning and management, like promoting sustainable mobility and bicycle use, it is possible to improve air quality and reduce air pollution. The project has the added benefits included in socially implying members of the community; some of the actions will be based on the help provided by a team of volunteer cyclists. This way citizens will become the driving force and main beneficiaries of the results. All the steps proposed in the project are directly related to the improvement of air quality, urban environment and health."""

    def update(self, doc):
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        doc.discipline = 'Environmental Monitoring'
        doc.resource_type = 'Dataset'
