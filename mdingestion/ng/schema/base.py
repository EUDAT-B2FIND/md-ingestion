from abc import ABC, abstractmethod
import shapely
from dateutil import parser as date_parser
from pathlib import Path

from .. import format
from ..parser import XMLParser, JSONParser


class B2FindDocument(ABC):
    """
    This is an abstract class defining a document for the B2Find schema.
    """

    @property
    @abstractmethod
    def title(self):
        pass

    @property
    @abstractmethod
    def description(self):
        pass

    @property
    @abstractmethod
    def tags(self):
        pass

    @property
    @abstractmethod
    def doi(self):
        pass

    @property
    @abstractmethod
    def source(self):
        pass

    @property
    @abstractmethod
    def related_identifier(self):
        pass

    @property
    @abstractmethod
    def metadata_access(self):
        pass

    @property
    @abstractmethod
    def creator(self):
        pass

    @property
    @abstractmethod
    def publisher(self):
        pass

    @property
    @abstractmethod
    def contributor(self):
        pass

    @property
    @abstractmethod
    def publication_year(self):
        pass

    @property
    @abstractmethod
    def rights(self):
        pass

    @property
    @abstractmethod
    def open_access(self):
        pass

    @property
    @abstractmethod
    def contact(self):
        pass

    @property
    @abstractmethod
    def language(self):
        pass

    @property
    @abstractmethod
    def resource_type(self):
        pass

    @property
    @abstractmethod
    def format(self):
        pass

    @property
    @abstractmethod
    def discipline(self):
        pass

    @property
    @abstractmethod
    def spatial_coverage(self):
        pass

    @property
    @abstractmethod
    def temporal_coverage_begin(self):
        pass

    @property
    @abstractmethod
    def temporal_coverage_end(self):
        pass

    @abstractmethod
    def json(self):
        pass


class BaseDocument(B2FindDocument):
    """
    This is an abstract class defining defaults and common methods for the B2Find document.
    """

    def __init__(self, filename, url=None, community=None, mdprefix=None):
        self.filename = filename
        self.url = url
        self.community = community
        self.mdprefix = mdprefix
        self._geometry = None
        self._start_date = None
        self._end_date = None
        self._parser = None

    @property
    def doc(self):
        return self._parser.doc

    def find(self, name=None, type=None, one=False, **kwargs):
        return self._parser.find(name, type, one, **kwargs)

    @property
    def name(self):
        return Path(self.filename).stem

    @property
    def metadata_access(self):
        return ''

    @property
    def discipline(self):
        return 'Various'

    @property
    def spatial_coverage(self):
        if not self.geometry:
            return ''
        return self.geometry.wkt

    @property
    def spatial(self):
        return format.format_string(self.bbox)

    @property
    def geometry(self):
        if not self._geometry:
            self._geometry = self.parse_geometry()
        return self._geometry

    def parse_geometry(self):
        raise NotImplementedError

    @property
    def geojson(self):
        if not self.geometry:
            return ''
        return shapely.geometry.mapping(self.geometry)

    @property
    def bbox(self):
        if not self.geometry:
            return None
        bbox = shapely.geometry.box(*self.geometry.bounds)
        return shapely.geometry.mapping(bbox)

    @property
    def temporal_coverage(self):
        return self.temporal_coverage_begin_date

    @property
    def temporal_coverage_begin_date(self):
        try:
            datestr = self.start_date.isoformat(timespec='seconds')
            datestr = f"{datestr}Z"
        except Exception:
            datestr = ''
        return datestr

    @property
    def temporal_coverage_end_date(self):
        try:
            datestr = self.end_date.isoformat(timespec='seconds')
            datestr = f"{datestr}Z"
        except Exception:
            datestr = ''
        return datestr

    @property
    def temp_coverage_begin(self):
        try:
            tstamp = int(self.start_date.timestamp())
        except Exception:
            tstamp = None
        return tstamp

    @property
    def temp_coverage_end(self):
        try:
            tstamp = int(self.end_date.timestamp())
        except Exception:
            tstamp = None
        return tstamp

    @property
    def start_date(self):
        if not self._start_date:
            self._start_date = date_parser.parse(self.temporal_coverage_begin)
        return self._start_date

    @property
    def end_date(self):
        if not self._end_date:
            self._end_date = date_parser.parse(self.temporal_coverage_end)
        return self._end_date

    @property
    def doi(self):
        return ''

    @property
    def fulltext(self):
        return self._parser.fulltext

    @classmethod
    def extension(cls):
        raise NotImplementedError

    def json(self):
        return {
            'title': self.title,
            'notes': self.description,
            'tags': self.tags,
            'DOI': self.doi,
            'url': self.source,
            'RelatedIdentifier': self.related_identifier,
            'MetaDataAccess': self.metadata_access,
            'author': self.creator,
            'Contributor': self.contributor,
            'Publisher': self.publisher,
            'PublicationYear': self.publication_year,
            'Rights': self.rights,
            'OpenAccess': self.open_access,
            'Contact': self.contact,
            'Language': self.language,
            'ResourceType': self.resource_type,
            'Format': self.format,
            'Discipline': self.discipline,
            'DiscHierarchy': [],
            'SpatialCoverage': self.spatial_coverage,
            'spatial': self.spatial,
            'TemporalCoverage': self.temporal_coverage,
            'TemporalCoverage:BeginDate': self.temporal_coverage_begin_date,
            'TemporalCoverage:EndDate': self.temporal_coverage_end_date,
            'TempCoverageBegin': self.temp_coverage_begin,
            'TempCoverageEnd': self.temp_coverage_end,
        }


class XMLMapper(BaseDocument):

    def __init__(self, filename, url=None, community=None, mdprefix=None):
        super().__init__(filename, url, community, mdprefix)
        self._parser = XMLParser(filename)

    @property
    def doc(self):
        return self._parser.doc

    def find(self, name=None, type=None, one=False, **kwargs):
        return self._parser.find(name, type, one, **kwargs)

    @classmethod
    def extension(cls):
        return XMLParser.extension()


class OAIMapper(XMLMapper):
    @property
    def oai_set(self):
        return self.find('setSpec', limit=1)

    @property
    def oai_identifier(self):
        return self.find('identifier', limit=1)

    @property
    def metadata_access(self):
        if self.oai_identifier:
            mdaccess = f"{self.url}?verb=GetRecord&metadataPrefix={self.mdprefix}&identifier={self.oai_identifier[0]}"  # noqa
        else:
            mdaccess = ''
        return mdaccess

    def json(self):
        _json = super().json()
        _json['oai_set'] = self.oai_set
        _json['oai_identifier'] = self.oai_identifier
        return _json


class JSONMapper(BaseDocument):
    def __init__(self, filename, url=None, community=None, mdprefix=None):
        super().__init__(filename, url, community, mdprefix)
        self._parser = JSONParser(filename)

    @classmethod
    def extension(cls):
        return JSONParser.extension
