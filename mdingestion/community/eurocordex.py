from .base import Repository
from ..service_types import SchemaType, ServiceType


class EUROCordex(Repository):
    IDENTIFIER = 'eurocordex'
    TITLE = 'EURO-CORDEX'
    GROUP = 'b2share'
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = 'a140d3f3-0117-4665-9945-4c7fcb9afb51'
    PRODUCTIVE = False
    DATE = '2023-06-18'
    EPOSITORY_ID = 're3data:r3d100013118'
    REPOSITORY_NAME = 'B2SHARE Server Forschungszentrum Jülich'
    DESCRIPTION = """EURO-CORDEX is the European branch of the international CORDEX initiative, which is a program sponsored by the World Climate Research Program (WRCP) to organize an internationally coordinated framework to produce improved regional climate change projections for all land regions world-wide. The CORDEX-results will serve as input for climate change impact and adaptation studies within the timeline of the Fifth Assessment Report (AR5) of the Intergovernmental Panel on Climate Change (IPCC) and beyond."""
    LINK = 'https://www.euro-cordex.net/'

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        if not doc.discipline:
            doc.discipline = 'Earth System Research'
