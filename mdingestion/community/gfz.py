from shapely.geometry import shape
import json
import pandas as pd
import os
import copy

from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


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


    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geosciences')


class Gfzdb(BaseGfz):
    IDENTIFIER = 'gfzdataservices'
    TITLE = 'GFZ Data Services'
    OAI_SET = 'DOIDB.GFZ'


class GfzCrc1211db(BaseGfz):
    IDENTIFIER = 'crc1211db'
    TITLE = 'CRC 1211 Database'
    OAI_SET = 'DOIDB.CRC1211'


class GfzEnmap(BaseGfz):
    IDENTIFIER = 'enmap'
    TITLE = 'EnMAP'
    OAI_SET = 'DOIDB.ENMAP'


class GfzFidgeo(BaseGfz):
    IDENTIFIER = 'fidgeo'
    TITLE = 'FID GEO'
    OAI_SET = 'DOIDB.FID'


class GfzGeofon(BaseGfz):
    IDENTIFIER = 'geofon'
    TITLE = 'GEOFON Seismic Networks'
    OAI_SET = 'DOIDB.GEOFON'


class GfzGipp(BaseGfz):
    IDENTIFIER = 'gipp'
    Title = 'GIPP - Geophysical Instrument Pool Potsdam'
    OAI_SET = 'DOIDB.GIPP'


class GfzIcgem(BaseGfz):
    IDENTIFIER = 'icgem'
    TITLE = 'ICGEM - International Centre for Global Earth Models'
    OAI_SET = 'DOIDB.ICGEM'


class GfzIgets(BaseGfz):
    IDENTIFIER = 'igets'
    TITLE = 'IGETS - International Geodynamics and Earth Tide Service'
    OAI_SET = 'DOIDB.IGETS'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geodesy, Geoinformatics and Remote Sensing')


class GfzIntermagnet(BaseGfz):
    IDENTIFIER = 'intermagnet'
    TITLE = 'INTERMAGNET'
    OAI_SET = 'DOIDB.INTERMAG'


# class GfzIsdc(BaseGfz):
#    IDENTIFIER = 'isdc'
#    OAI_SET = ' DOIDB.ISDC'


class GfzIsg(BaseGfz):
    IDENTIFIER = 'isg'
    TITLE = 'ISG - International Service for the Geoid'
    OAI_SET = 'DOIDB.ISG'


class GfzPik(BaseGfz):
    IDENTIFIER = 'pik'
    TITLE = 'PIK - Potsdam Institute for Climate Impact Research'
    OAI_SET = 'DOIDB.PIK'


class GfzTereno(BaseGfz):
    IDENTIFIER = 'tereno'
    TITLE = 'TERENO'
    OAI_SET = 'DOIDB.TERENO'


class GfzWsm(BaseGfz):
    IDENTIFIER = 'wsm'
    TITLE = 'WSM - World Stress Map'
    OAI_SET = 'DOIDB.WSM'
