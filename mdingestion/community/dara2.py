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
        doc.doi = self.doi()
        doc.creator = self.creator()
        doc.description = self.description()
        '''
        doc.publication_year = self._find('publicationYear')
        doc.rights = self._find('rights')
        doc.contributor = self._find('contributors')
        doc.funding_reference = self._find('fundingReferences')
        doc.related_identifier = self._find('relatedIdentifiers')
        doc.format = self._find('formats')
        doc.size = self._find('sizes')
        doc.resource_type = self._find('resourceTypeGeneral')
        doc.version = self._find('version')
        doc.title = self._find('title')
        doc.language = self._find('language')
        doc.keywords = self._find('subject')
        doc.publisher = self._find('publisher')
        doc.places = self._find('geoLocationPlace')
        '''
    @property
    def jsondoc(self):
        return self.reader.parser.doc

    def doi(self):
        try:
            doi_ = self.jsondoc['attributes']['doi']
        except Exception as e:
            doi_ = None
        return doi_

    def creator(self):
        try:
            creator_ = [c['name'] for c in self.jsondoc['attributes']['creators']]
        except Exception as e:
            creator_ = None
        return creator_

    def description(self):
        try:
            description_ = [c['description'] for c in self.jsondoc['attributes']['descriptions']]
        except Exception as e:
            description_ = None
        return description_