from .base import Repository
from ..service_types import SchemaType, ServiceType


class DARADatacite(Repository):
    IDENTIFIER = 'dara2'
    TITLE = 'DA|RA'
    URL = 'https://api.datacite.org/'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.DataCite
    FILTER = "daraco"
    PRODUCTIVE = True
    DATE = '2025-04-02'
    CRON_DAILY = False
    REPOSITORY_ID = ''
    REPOSITORY_NAME = 'DA|RA'
    LOGO = ""
    DESCRIPTION = ""

    def update(self, doc):
        doc.discipline = ['Social Sciences']
        doc.doi = self.find('attributes.doi')
        doc.description = self._find('Abstract')
        doc.source = self._find('OnlineResourceUrl')
        doc.publication_year = self._find('Last_Update')
        doc.contributor = self._find('Organisations')
        doc.contact = ['blue-cloud-support@maris.nl']
        doc.title = self._find('Title') or self._find('Abstract')
        doc.temporal_coverage_begin_date = self._find('Temporal_Extent_Begin')
        doc.temporal_coverage_end_date = self._find('Temporal_Extent_End')
        doc.geometry = self.geometry()
        doc.instrument = self.instruments()
        doc.keywords = self.keywords()
        doc.publisher = self.publishers()


    def creator(self):
        creators = []
        for creator in self.parser.doc.find_all('creator'):
            name = creator.creatorName.text
            orcid_element = creator.find('nameIdentifier', nameIdentifierScheme="ORCID")
            if orcid_element:
                orcid = orcid_element.text
                c = f"{name} (ORCID: {orcid})"
            else:
                c = name
            creators.append(c)
        return creators

