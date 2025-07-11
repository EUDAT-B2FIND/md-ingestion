from .base import Repository
from ..service_types import SchemaType, ServiceType


class BaseDrihm(Repository):
    # old/finished project, no cronjob needed
    NAME = 'drihm'
    TITLE = 'DRIHM'
    PRODUCTIVE = True
    DATE = '2023-05-10'
    LINK = "http://www.drihm.eu/"
    LOGO = "http://www.drihm.eu/images/modules/LogoDrihm.jpg"
    DESCRIPTION = """The Distributed Research Infrastructure for Hydro-Meteorology (DRIHM) project intends to develop a prototype e-Science environment to facilitate this collaboration and provide end-to-end HMR services (models, datasets and post-processing tools) at the European level, with the ability to expand to global scale. The objectives of DRIHM are to lead the definition of a common long-term strategy, to foster the development of new HMR models and observational archives for the study of severe hydrometeorological events, to promote the execution and analysis of high-end simulations, and to support thviaxxsettembre2e dissemination of predictive models as decision analysis tools."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        doc.discipline = self.discipline(doc, 'Meteorology')


class DrihmCsc(BaseDrihm):
    IDENTIFIER = 'drihm_csc'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_SET = '94a9567e-2fba-4677-8fde-a8b68bdb63e8'  # DRIHM Set from CSC
    OAI_METADATA_PREFIX = 'eudatcore'
    REPOSITORY_ID = 're3data:r3d100011394'
    REPOSITORY_NAME = 'B2SHARE'


class DrihmFzj(BaseDrihm):
    IDENTIFIER = 'drihm_fzj'
    GROUP = 'b2share'
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_SET = '94a9567e-2fba-4677-8fde-a8b68bdb63e8'  # DRIHM Set from FZJ
    OAI_METADATA_PREFIX = 'eudatcore'
    REPOSITORY_ID = 're3data:r3d100013118'
    REPOSITORY_NAME = 'B2SHARE Server Forschungszentrum Jülich'
