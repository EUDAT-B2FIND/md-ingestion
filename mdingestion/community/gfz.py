from shapely.geometry import shape
from .base import Repository
from ..service_types import SchemaType, ServiceType


class BaseGfz(Repository):
    GROUP = 'gfz'
    GROUP_TITLE = 'GFZ Data Services'
    PRODUCTIVE = True
    DATE = '2021-08-15'
    DESCRIPTION = 'By the GFZ Data Services the GFZ archives and publishes datasets to which a DOI has been assigned and which cover all geoscientific disciplines.'
    LOGO = ''
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    REPOSITORY_ID = 're3data:r3d100012335'
    REPOSITORY_NAME = 'GFZ Data Services'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geosciences')


class Gfzdb(BaseGfz):
    IDENTIFIER = 'gfzdataservices'
    TITLE = 'GFZ Data Services'
    OAI_SET = 'DOIDB.GFZ'
    REPOSITORY_ID = 're3data:r3d100012335'
    REPOSITORY_NAME = 'GFZ Data Services'

# class GfzIsdc(BaseGfz):
#    IDENTIFIER = 'isdc'
#    OAI_SET = ' DOIDB.ISDC'
