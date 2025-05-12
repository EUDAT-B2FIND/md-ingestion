from .base import Repository
from ..service_types import SchemaType, ServiceType


def return_none_on_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            return None
    return wrapper


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
        doc.title = self.titles()
        doc.publisher = self.publisher()
        doc.description = self.description()
        doc.places = self.geolocation_place()
        doc.publication_year = self.publication_year()
        doc.keywords = self.subjects()
        doc.language = self.language()
        doc.rights = self.rights()
        doc.contributor = self.contributor()
        doc.funding_reference = self.funding_reference()
        doc.related_identifier = self.related_identifier()
        doc.format = self.formats()
        doc.size = self.sizes()
        doc.resource_type = self.resourcetype_general()
        doc.version = self.version()

    @property
    def jsondoc(self):
        return self.reader.parser.doc['attributes']

    @return_none_on_exception
    def doi(self):
        value = self.jsondoc['doi']
        return value

    @return_none_on_exception
    def creator(self):
        value = [c['name'] for c in self.jsondoc['creators']]
        return value

    @return_none_on_exception
    def titles(self):
        value = [c['title'] for c in self.jsondoc['titles']]
        return value

    @return_none_on_exception
    def publisher(self):
        value = self.jsondoc['publisher']
        return value

    @return_none_on_exception
    def description(self):
        value = [c['description'] for c in self.jsondoc['descriptions']]
        return value

    @return_none_on_exception
    def geolocation_place(self):
        value = [c['geoLocationPlace'] for c in self.jsondoc['geoLocations']]
        return value

    @return_none_on_exception
    def publication_year(self):
        value = self.jsondoc['publicationYear']
        return value

    @return_none_on_exception
    def subjects(self):
        value = [c['subject'] for c in self.jsondoc['subjects']]
        return value

    @return_none_on_exception
    def language(self):
        value = self.jsondoc['language']
        return value

    @return_none_on_exception
    def rights(self):
        value = [c['rights'] for c in self.jsondoc['rightsList']]
        return value

    @return_none_on_exception
    def contributor(self):
        value = [c['name'] for c in self.jsondoc['contributors']]
        return value

    @return_none_on_exception
    def funding_reference(self):
        values = []
        for c in self.jsondoc['fundingReferences']:
            value = c['funderName']
            awnu = c.get('awardNumber')
            if awnu:
                value += f": {awnu}"
            values.append(value)
        return values

    @return_none_on_exception
    def related_identifier(self):
        values = []
        for c in self.jsondoc['relatedIdentifiers']:
            id = c['relatedIdentifier']
            relid_type = c.get('relatedIdentifierType')
            rel_type = c.get('relationType')
            value = f"{id}|{relid_type}|{rel_type}"
            values.append(value)
        return values

    @return_none_on_exception
    def formats(self):
        value = self.jsondoc['formats']
        return value

    @return_none_on_exception
    def sizes(self):
        value = self.jsondoc['sizes']
        return value

    @return_none_on_exception
    def resourcetype_general(self):
        value = self.jsondoc['types']['resourceTypeGeneral']
        return value

    @return_none_on_exception
    def version(self):
        value = self.jsondoc['version']
        return value
