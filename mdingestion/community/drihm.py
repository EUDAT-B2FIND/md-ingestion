from .base import Repository
from ..service_types import SchemaType, ServiceType


class DrihmEudatcore(Repository):
    IDENTIFIER = 'drihm'
    TITLE = 'DRIHM'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '94a9567e-2fba-4677-8fde-a8b68bdb63e8'
    PRODUCTIVE = True
    DATE = '2023-05-10'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False  # old/finished project, no cronjob needed
    LOGO = "http://www.drihm.eu/images/modules/LogoDrihm.jpg"
    LINK = "http://www.drihm.eu/"
    DESCRIPTION = """The Distributed Research Infrastructure for Hydro-Meteorology (DRIHM) project intends to develop a prototype e-Science environment to facilitate this collaboration and provide end-to-end HMR services (models, datasets and post-processing tools) at the European level, with the ability to expand to global scale. The objectives of DRIHM are to lead the definition of a common long-term strategy, to foster the development of new HMR models and observational archives for the study of severe hydrometeorological events, to promote the execution and analysis of high-end simulations, and to support thviaxxsettembre2e dissemination of predictive models as decision analysis tools."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        doc.discipline = self.discipline(doc, 'Meteorology')
