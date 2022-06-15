from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class BaseGfz(Community):
    NAME = 'gfz'
    IDENTIFIER = NAME
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.GFZ'
    PRODUCTIVE = False

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geosciences')

class GfzCrc1211db(BaseGfz):
    GROUP = 'crc1211db'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.CRC1211' 

    #def update(self, doc):
     #   doc.discipline = self.discipline(doc, 'Public Health, Health Services Research, Social Medicine')

class GfzEnmap(BaseGfz):
    GROUP = 'enmap'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.ENMAP'

class GfzFidgeo(BaseGfz):
    GROUP = 'fidgeo'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.FID'

class GfzGeofon(BaseGfz):
    GROUP = 'geofon'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.GEOFON'

class GfzGipp(BaseGfz):
    GROUP = 'gipp'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.GIPP'

class GfzIcgem(BaseGfz):
    GROUP = 'icgem'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.ICGEM'

class GfzIgets(BaseGfz):
    GROUP = 'igets'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.IGETS'

class GfzIntermagnet(BaseGfz):
    GROUP = 'intermagnet'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.INTERMAG'

#class GfzIsdc(BaseGfz):
#    GROUP = 'isdc'
#    IDENTIFIER = GROUP
#    OAI_SET = ' DOIDB.ISDC'

class GfzIsg(BaseGfz):
    GROUP = 'isg'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.ISG'

class GfzPik(BaseGfz):
    GROUP = 'pik'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.PIK'

class GfzGeofon(BaseGfz):
    GROUP = 'geofon'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.SEISNET'

class GfzTereno(BaseGfz):
    GROUP = 'tereno'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.TERENO'

class GfzWsm(BaseGfz):
    GROUP = 'wsm'
    IDENTIFIER = GROUP
    OAI_SET = 'DOIDB.WSM'