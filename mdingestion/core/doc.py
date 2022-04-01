from abc import ABC, abstractmethod
import shapely
from shapely import wkt
from dateutil import parser as date_parser
from pathlib import Path
import json

from ..format import format_value
from ..util import utc2seconds
from ..rights import is_open_access


class BaseDoc(object):
    def __init__(self):
        self._community = None
        self._group = None
        self._title = None
        self._description = None
        self._keywords = None
        self._doi = None
        self._pid = None
        self._source = None
        self._related_identifier = None
        self._metadata_access = None
        self._creator = None
        self._publisher = None
        self._contributor = None
        self._instrument = None
        self._publication_year = None
        self._funding_reference = None
        self._rights = None
        self._open_access = None
        self._contact = None
        self._language = None
        self._resource_type = None
        self._format = None
        self._size = None
        self._version = None
        self._discipline = None

    @property
    def community(self):
        return self._community

    @property
    def group(self):
        return self._group

    @group.setter
    def group(self, value):
        self._group = format_value(value)

    @property
    def identifier(self):
        return self._doi or self._pid or self._source

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, value):
        self._title = format_value(value)

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        self._description = format_value(value)

    @property
    def keywords(self):
        return self._keywords

    @keywords.setter
    def keywords(self, value):
        # TODO: clean up code
        _keywords = []
        delchars = {8347: None}
        for val in value:
            val = val.translate(delchars)
            _keywords.extend(val.split(','))
        self._keywords = format_value(_keywords, type='string_words', min_length=2, max_length=100)

    @property
    def doi(self):
        return self._doi or ''

    @doi.setter
    def doi(self, value):
        self._doi = format_value(value, type='url', one=True)

    @property
    def pid(self):
        return self._pid or ''

    @pid.setter
    def pid(self, value):
        self._pid = format_value(value, type='url', one=True)

    @property
    def source(self):
        url = self._source or ''
        if url in [self.pid, self.doi]:
            url = ''
        return url

    @source.setter
    def source(self, value):
        self._source = format_value(value, type='url', one=True)

    @property
    def related_identifier(self):
        urls = []
        if self._related_identifier:
            for url in self._related_identifier:
                if self.doi and self.doi in url:
                    continue
                if self.pid and self.pid in url:
                    continue
                if self.source and self.source in url:
                    continue
                if self.metadata_access and self.metadata_access in url:
                    continue
                urls.append(url)
        return urls

    @related_identifier.setter
    def related_identifier(self, value):
        self._related_identifier = format_value(value, type='url')

    @property
    def metadata_access(self):
        return self._metadata_access

    @metadata_access.setter
    def metadata_access(self, value):
        self._metadata_access = format_value(value, type='url', one=True)

    @property
    def creator(self):
        return self._creator

    @creator.setter
    def creator(self, value):
        self._creator = format_value(value)

    @property
    def publisher(self):
        return self._publisher

    @publisher.setter
    def publisher(self, value):
        self._publisher = format_value(value)

    @property
    def contributor(self):
        return self._contributor

    @contributor.setter
    def contributor(self, value):
        self._contributor = format_value(value, type='email')

    @property
    def instrument(self):
        return self._instrument

    @instrument.setter
    def instrument(self, value):
        self._instrument = format_value(value)

    @property
    def publication_year(self):
        return self._publication_year

    @publication_year.setter
    def publication_year(self, value):
        self._publication_year = format_value(value, type='date_year', one=True)

    @property
    def funding_reference(self):
        return self._funding_reference

    @funding_reference.setter
    def funding_reference(self, value):
        self._funding_reference = format_value(value)

    @property
    def rights(self):
        return self._rights

    @rights.setter
    def rights(self, value):
        self._rights = format_value(value)

    @property
    def open_access(self):
        return is_open_access(self.rights)

    # @open_access.setter
    # def open_access(self, value):
    #     self._open_access = format_value(value, type='boolean', one=True)

    @property
    def contact(self):
        return self._contact

    @contact.setter
    def contact(self, value):
        self._contact = format_value(value, type='email')

    @property
    def language(self):
        return self._language

    @language.setter
    def language(self, value):
        self._language = format_value(value, type='language')

    @property
    def resource_type(self):
        return self._resource_type

    @resource_type.setter
    def resource_type(self, value):
        self._resource_type = format_value(value)

    @property
    def format(self):
        return self._format

    @format.setter
    def format(self, value):
        self._format = format_value(value)

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, value):
        self._size = format_value(value)

    @property
    def version(self):
        return self._version or ''

    @version.setter
    def version(self, value):
        self._version = format_value(value, one=True)

    @property
    def discipline(self):
        return self._discipline or 'Other'

    @discipline.setter
    def discipline(self, value):
        self._discipline = format_value(value)


class GeoDoc(BaseDoc):
    def __init__(self):
        super().__init__()
        self._geometry = None
        self._places = None
        self._temporal_coverage = None
        self._begin_date = None
        self._end_date = None

    @property
    def spatial_coverage(self):
        coverage = ''
        if self.geometry:
            coverage = self.format_coverage()
        if self.places:
            if coverage:
                coverage += '; '
            coverage += '; '.join(self.places)
        return coverage

    def format_coverage(self):
        geom = ''
        if self.geometry:
            if self.geometry.geom_type == 'Point':
                point = self.geometry
                geom = f"({point.x:.3f} LON, {point.y:.3f} LAT)"
            else:
                bounds = self.geometry.bounds
                # print(f"{bounds}")
                geom = f"({bounds[0]:.3f}W, {bounds[1]:.3f}S, {bounds[2]:.3f}E, {bounds[3]:.3f}N)"
        return geom

    @property
    def wkt(self):
        if not self.geometry:
            return None
        return wkt.dumps(self.geometry)

    @property
    def wkt_simple(self):
        if not self.geometry:
            return None
        return wkt.dumps(self.geometry.centroid)

    @property
    def geometry(self):
        return self._geometry

    @geometry.setter
    def geometry(self, value):
        self._geometry = value

    @property
    def places(self):
        return self._places

    @places.setter
    def places(self, value):
        self._places = value

    @property
    def bbox(self):
        if not self.geometry:
            return None
        bbox = shapely.geometry.box(*self.geometry.bounds)
        return shapely.geometry.mapping(bbox)

    @property
    def envelope(self):
        if not self.geometry:
            return None
        # bounds: minx, miny, maxx, maxy
        # envelop: minX, maxX, maxY, minY
        return "ENVELOPE({0}, {2}, {3}, {1})".format(
            *self.geometry.bounds)

    @property
    def temporal_coverage(self):
        """string like Viking Age e.g."""
        return self._temporal_coverage

    @temporal_coverage.setter
    def temporal_coverage(self, value):
        val = format_value(value, one=True)
        if '/' in val:
            begin, end = val.split('/', 1)
        else:
            begin = val
            end = None
        # print(begin, end)
        self.temporal_coverage_begin_date = begin
        self.temporal_coverage_end_date = end
        if not self.temporal_coverage_begin_date:
            self._temporal_coverage = val

    @property
    def temporal_coverage_begin_date(self):
        """field begin datetime in utc format in single record"""
        return self._begin_date or ''

    @temporal_coverage_begin_date.setter
    def temporal_coverage_begin_date(self, value):
        self._begin_date = format_value(value, type='datetime', one=True)

    @property
    def temporal_coverage_end_date(self):
        """field end datetime in utc format in single record"""
        return self._end_date or ''

    @temporal_coverage_end_date.setter
    def temporal_coverage_end_date(self, value):
        self._end_date = format_value(value, type='datetime', one=True)

    @property
    def temp_coverage_begin(self):
        """Mikail's seconds since B.C. extension - TODO"""
        try:
            # tstamp = int(date_parser.parse(self._begin_date).timestamp())
            tstamp = utc2seconds(self.temporal_coverage_begin_date)
        except Exception:
            tstamp = None
        return tstamp

    @property
    def temp_coverage_end(self):
        try:
            # tstamp = int(date_parser.parse(self._end_date).timestamp())
            tstamp = utc2seconds(self.temporal_coverage_end_date)
        except Exception:
            tstamp = None
        return tstamp


class B2FDoc(GeoDoc):

    def __init__(self, filename, community=None, url=None, oai_metadata_prefix=None):
        super().__init__()
        self.filename = filename
        self._community = community
        self._url = url
        self._oai_metadata_prefix = oai_metadata_prefix
        self._oai_set = None
        self._oai_identifier = None
        self._file_identifier = None
        self._fulltext = None

    @property
    def name(self):
        return Path(self.filename).stem

    @property
    def url(self):
        return self._url

    @property
    def fulltext(self):
        return self._fulltext

    @fulltext.setter
    def fulltext(self, value):
        self._fulltext = value

    @property
    def oai_metadata_prefix(self):
        return self._oai_metadata_prefix

    @property
    def oai_set(self):
        return self._oai_set

    @oai_set.setter
    def oai_set(self, value):
        self._oai_set = format_value(value, one=True)

    @property
    def oai_identifier(self):
        return self._oai_identifier

    @oai_identifier.setter
    def oai_identifier(self, value):
        self._oai_identifier = format_value(value, one=True)

    @property
    def file_identifier(self):
        return self._file_identifier

    @file_identifier.setter
    def file_identifier(self, value):
        self._file_identifier = format_value(value, one=True)
