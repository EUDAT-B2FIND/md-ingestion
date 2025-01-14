from .base import Repository
from ..service_types import SchemaType, ServiceType

# repository not on productive B2F, contains 0 records (since 2023 - dead?)

class GeoinquireEudatcore(Repository):
    IDENTIFIER = 'geoinquire'
    TITLE = 'Geo-INQUIRE'
    # GROUP = 'b2share'
    CRON_DAILY = True
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '21b8d496-ac37-4929-adc3-1dfb312ce5e5'  # geoInquire Set from CSC
    PRODUCTIVE = False
    DATE = '2023-07-20'
    LOGO = "https://www.geo-inquire.eu/typo3conf/ext/geoinquire_sitepackage/Resources/Public/Images/Geo-INQUIRE_logo_2_crop.jpg"
    LINK = "https://www.geo-inquire.eu/"
    DESCRIPTION = """Modern scientific endeavours already have the capacity to call upon a vast variety of data, often in huge volumes. However, the challenge is not only how to make the most of such a resource, but also how to make it available to the wider scientific community, especially when encouraging curiosity-driven research. The new project, Geosphere INfrastructures for QUestions into Integrated REsearch part of the recent Horizon Europe Infrastructure call, will do so by enhancing, giving access to, and making interoperable key datasets."""

    def update(self, doc):
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
