from .base import Repository
from ..service_types import SchemaType, ServiceType


class IngridDublincore(Repository):
    IDENTIFIER = 'ingrid'
    TITLE = 'InGRID'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = '0cc56898-fb0b-44dc-8331-535b9c089647'  # InGRID Set from CSC
    PRODUCTIVE = False
    DATE = '2023-01-23'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False
    LOGO = ""
    DESCRIPTION = """InGRID is a network of distributed, but integrating European research infrastructures. A research infrastructure is a facility or platform that provides the scientific community with resources and services to conduct top-level research in their respective fields. InGRID research infrastructures serve the social sciences community, that wants to make an evidence-based contribution to a European policy strategy of inclusive growth. This research community focuses on social in/exclusion, vulnerability-at-work and related social and labour market policies from a European comparative perspective. Key tools in this social science research are all types of data: statistics on earnings, administrative social data, labour market data, surveys of quality of life or working conditions, and policy indicators. For the period 2017-2021, the infrastructure has received funding for another 4 year project by the European H2020-programme: the InGRID-2 project. As a continuation of the launch of the infrastructure in 2013, this project will work on the infrastructure as an advanced research infrastructure. The full project title is "InGRID-2 Integrating Research Infrastructure for European expertise on Inclusive Growth from data to policy". To improve and open the research infrastructures, the InGRID-2 project built this online portal and organises several activities. """

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        if not doc.discipline:
            doc.discipline = 'Social Sciences'
