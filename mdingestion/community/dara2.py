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
        doc.publication_year = self.publication_year()
        doc.rights = self.rights()
        doc.contributor = self.contributor()
        doc.funding_reference = self.funding_reference()
        doc.related_identifier = related_identifier()
        doc.format = self._find('formats')
        doc.size = self._find('sizes')
        doc.resource_type = self._find('resourceTypeGeneral')
        doc.version = self._find('version')
        doc.title = self._find('title')
        doc.language = self._find('language')
        doc.keywords = self._find('subject')
        doc.publisher = self._find('publisher')
        doc.places = self._find('geoLocationPlace')

    @property
    def jsondoc(self):
        return self.reader.parser.doc['attributes']

    def doi(self):
        try:
            value = self.jsondoc['doi']
        except Exception as e:
            value = None
        return value

    def creator(self):
        try:
            value = [c['name'] for c in self.jsondoc['creators']]
        except Exception as e:
            value = None
        return value

    def description(self):
        try:
            value = [c['description'] for c in self.jsondoc['descriptions']]
        except Exception as e:
            value = None
        return value

    def publication_year(self):
        try:
            value = self.jsondoc['publicationYear']
        except Exception as e:
            value = None
        return value

    def rights(self):
        try:
            value = [c['rights'] for c in self.jsondoc['rightsList']]
        except Exception as e:
            value = None
        return value

    def contributor(self):
        try:
            value = [c['name'] for c in self.jsondoc['contributors']]
        except Exception as e:
            value = None
        return value

    def funding_reference(self):
        try:
            values = []
            for c in self.jsondoc['fundingReferences']:
                value = c['funderName']
                awnu = c.get('awardNumber')
                if awnu:
                    value += f": {awnu}"
                values.append(value)
        except Exception as e:
            values = None
        return values

    def related_identifier(self):
        try:
            values = []
            for c in self.jsondoc['relatedIdentifiers']:
                id = c['relatedIdentifier']
                relid_type = c.get('relatedIdentifierType')
                rel_type = c.get('relationType')
                value = f"{id}|{relid_type}|{rel_type}"
                values.append(value)
        except Exception as e:
            values = None
        return values