from .base import Repository
from ..service_types import SchemaType, ServiceType


class BaseGeoInquire(Repository):
    IDENTIFIER = 'geoInquire'
    NAME = 'geoInquire'
    PRODUCTIVE = True
    DATE = '2023-07-20'
    CRON_DAILY = True
    LOGO = "https://www.geo-inquire.eu/typo3conf/ext/geoinquire_sitepackage/Resources/Public/Images/Geo-INQUIRE_logo_2_crop.jpg"
    LINK = "https://www.geo-inquire.eu/"

    def update(self, doc):
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')


class GeoInquireB2ShareCsc(BaseGeoInquire):
    IDENTIFIER = 'geoInquire_csc'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '21b8d496-ac37-4929-adc3-1dfb312ce5e5'  # geoInquire Set from CSC
