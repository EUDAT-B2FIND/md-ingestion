from shapely.geometry import shape
import json
import pandas as pd
import os
import copy

from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class BaseGfz(Community):
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


    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geosciences')


class Gfzdb(BaseGfz):
    NAME = 'gfzdataservices'
    TITLE = 'GFZ Data Services'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.GFZ'


class GfzCrc1211db(BaseGfz):
    NAME = 'crc1211db'
    TITLE = 'CRC 1211 Database'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.CRC1211'


class GfzEnmap(BaseGfz):
    NAME = 'enmap'
    TITLE = 'EnMAP'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.ENMAP'


class GfzFidgeo(BaseGfz):
    NAME = 'fidgeo'
    TITLE = 'FID GEO'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.FID'


class GfzGeofon(BaseGfz):
    NAME = 'geofon'
    TITLE = 'GEOFON Seismic Networks'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.GEOFON'


class GfzGipp(BaseGfz):
    NAME = 'gipp'
    Title = 'GIPP - Geophysical Instrument Pool Potsdam'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.GIPP'


class GfzIcgem(BaseGfz):
    NAME = 'icgem'
    TITLE = 'ICGEM - International Centre for Global Earth Models'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.ICGEM'


class GfzIgets(BaseGfz):
    NAME = 'igets'
    TITLE = 'IGETS - International Geodynamics and Earth Tide Service'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.IGETS'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geodesy, Geoinformatics and Remote Sensing')


class GfzIntermagnet(BaseGfz):
    NAME = 'intermagnet'
    TITLE = 'INTERMAGNET'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.INTERMAG'


# class GfzIsdc(BaseGfz):
#    NAME = 'isdc'
#    IDENTIFIER = NAME
#    OAI_SET = ' DOIDB.ISDC'


class GfzIsg(BaseGfz):
    NAME = 'isg'
    TITLE = 'ISG - International Service for the Geoid'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.ISG'


class GfzPik(BaseGfz):
    NAME = 'pik'
    TITLE = 'PIK - Potsdam Institute for Climate Impact Research'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.PIK'


class GfzTereno(BaseGfz):
    NAME = 'tereno'
    TITLE = 'TERENO'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.TERENO'


class GfzWsm(BaseGfz):
    NAME = 'wsm'
    TITLE = 'WSM - World Stress Map'
    IDENTIFIER = NAME
    OAI_SET = 'DOIDB.WSM'
