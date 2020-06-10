from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
import json
from jsonpath_ng import parse as parse_jsonpath
import shapely
from dateutil import parser as date_parser
from pathlib import Path

from .. import format


class ABCMapper(ABC):
    """
    This is an abstract class defining the mapping methods for the CKAN schema.
    """

    @property
    @abstractmethod
    def title(self):
        pass

    @property
    @abstractmethod
    def notes(self):
        pass

    @property
    @abstractmethod
    def tags(self):
        pass

    @property
    @abstractmethod
    def url(self):
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
    def author(self):
        pass

    @property
    @abstractmethod
    def contributor(self):
        pass

    @property
    @abstractmethod
    def publisher(self):
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
    def contact(self):
        pass

    @property
    @abstractmethod
    def open_access(self):
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

    @property
    @abstractmethod
    def fulltext(self):
        pass

    @property
    @abstractmethod
    def oai_set(self):
        pass

    @property
    @abstractmethod
    def oai_identifier(self):
        pass

    @property
    @abstractmethod
    def doi(self):
        pass


class BaseMapper(ABCMapper):
    """
    This is an abstract class defining defaults and common methods for the mapping classes.
    """

    def __init__(self, filename, url=None, community=None, mdprefix=None):
        self.filename = filename
        self.source = url
        self.community = community
        self.mdprefix = mdprefix
        self._doc = None
        self._geometry = None
        self._start_date = None
        self._end_date = None

    @property
    def doc(self):
        if self._doc is None:
            self._doc = self.parse_doc()
        return self._doc

    def parse_doc(self):
        raise NotImplementedError

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
    def fulltext(self):
        return ''

    @property
    def oai_set(self):
        return ''

    @property
    def oai_identifier(self):
        return ''

    @property
    def doi(self):
        return ''

    @property
    def name(self):
        return Path(self.filename).stem

    @classmethod
    def extension(cls):
        raise NotImplementedError

    def json(self):
        return {
            'title': self.title,
            'notes': self.notes,
            'tags': self.tags,
            'url': self.url,
            'RelatedIdentifier': self.related_identifier,
            'MetadataAccess': self.metadata_access,
            'author': self.author,
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
            'SpatialCoverage': self.spatial_coverage,
            'spatial': self.spatial,
            'TemporalCoverage': self.temporal_coverage,
            'TemporalCoverage:BeginDate': self.temporal_coverage_begin_date,
            'TemporalCoverage:EndDate': self.temporal_coverage_end_date,
            'TempCoverageBegin': self.temp_coverage_begin,
            'TempCoverageEnd': self.temp_coverage_end,
            'fulltext': self.fulltext,
            'oai_set': self.oai_set,
            'oai_identifier': self.oai_identifier,
            'DOI': self.doi,
            'name': self.name,
            'group': self.community,
            'groups': [{
                'name': self.community,
            }],
            'state': 'active',
        }


class XMLMapper(BaseMapper):

    def parse_doc(self):
        return BeautifulSoup(open(self.filename), 'xml')

    def find(self, name=None, type=None, one=False, **kwargs):
        tags = self.doc.find_all(name, **kwargs)
        formatted = [format.format(tag.text, type=type) for tag in tags]
        if one:
            if formatted:
                value = formatted[0]
            else:
                value = ''
        else:
            value = formatted
        return value

    @classmethod
    def extension(cls):
        return '.xml'

    @property
    def fulltext(self):
        lines = [txt.strip() for txt in self.doc.find_all(string=True)]
        lines_not_empty = [txt for txt in lines if len(txt) > 0]
        return lines_not_empty

    @property
    def oai_set(self):
        return self.find('setSpec', one=True)

    @property
    def oai_identifier(self):
        return self.find('identifier', limit=1, one=True)

    @property
    def metadata_access(self):
        mdaccess = f"{self.source}?verb=GetRecord&metadataPrefix={self.mdprefix}&identifier={self.oai_identifier}"
        return mdaccess


class JSONMapper(BaseMapper):
    EXPR_CACHE = {}

    def get_parseexpr(self, name):
        if name in JSONMapper.EXPR_CACHE:
            expr = JSONMapper.EXPR_CACHE[name]
        else:
            expr = parse_jsonpath(name)
            JSONMapper.EXPR_CACHE[name] = expr
        return expr

    def parse_doc(self):
        return json.load(open(self.filename))

    def find(self, name=None, type=None, one=False, **kwargs):
        expr = self.get_parseexpr(name)
        tags = expr.find(self.doc)
        formatted = [format.format(tag.value, type=type) for tag in tags]
        if one:
            if formatted:
                value = formatted[0]
            else:
                value = ''
        else:
            value = formatted
        return value

    @property
    def fulltext(self):
        """Pull all values from nested JSON."""
        arr = []

        def extract(obj, arr):
            """Recursively search for values in JSON tree."""
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        extract(v, arr)
                    else:
                        arr.append(v)
            elif isinstance(obj, list):
                for item in obj:
                    extract(item, arr)
            return arr

        results = extract(self.doc, arr)
        return results

    @classmethod
    def extension(cls):
        return '.json'
