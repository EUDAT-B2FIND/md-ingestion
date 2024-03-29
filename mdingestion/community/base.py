from ..reader import build_reader
from ..service_types import SchemaType, ServiceType


class BaseOrg(object):
    TITLE = None
    NAME = None
    IDENTIFIER = None
    URL = None
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    FILTER = None
    PRODUCTIVE = False
    DATE = ''
    DESCRIPTION = None
    LOGO = None
    CRON_DAILY = False
    REPOSITORY_ID = None
    REPOSITORY_NAME = None

    def __init__(self):
        self._reader = None

    @property
    def identifier(self):
        return self.IDENTIFIER

    @property
    def name(self):
        return self.NAME or self.IDENTIFIER

    @property
    def url(self):
        return self.URL

    @property
    def schema(self):
        return self.SCHEMA

    @property
    def service_type(self):
        return self.SERVICE_TYPE

    @property
    def oai_metadata_prefix(self):
        return self.OAI_METADATA_PREFIX

    @property
    def oai_set(self):
        return self.OAI_SET

    @property
    def filter(self):
        return self.FILTER

    @property
    def reader(self):
        if not self._reader:
            self._reader = build_reader(self.schema, self.service_type)
        return self._reader

    def read(self, filename):
        doc = self.reader.read(
            filename,
            repo=self.name,
            url=self.url,
            oai_metadata_prefix=self.oai_metadata_prefix,
            repository_id=self.REPOSITORY_ID,
            repository_name=self.REPOSITORY_NAME)
        doc.groups = self.group
        self.update(doc)
        return doc

    @property
    def extension(self):
        return self.reader.extension()

    def find(self, name=None, **kwargs):
        return self.reader.find(name=name, **kwargs)

    def find_ok(self, name=None, **kwargs):
        return self.reader.find_ok(name=name, **kwargs)

    def find_doi(self, name=None, **kwargs):
        return self.reader.find_doi(name=name, **kwargs)

    def find_pid(self, name=None, **kwargs):
        return self.reader.find_pid(name=name, **kwargs)

    def find_source(self, name=None, **kwargs):
        return self.reader.find_source(name=name, **kwargs)

    def discipline(self, doc, default=None):
        return self.reader.discipline(doc, default)

    @property
    def errors(self):
        return self.reader.errors

    def update(self, doc):
        pass

    def __str__(self):
        return self.IDENTIFIER

    def __repr__(self):
        return self.__str__()


class Repository(BaseOrg):
    GROUP = None
    GROUP_TITLE = None

    @property
    def group(self):
        return self.GROUP
