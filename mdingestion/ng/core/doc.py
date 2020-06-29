from abc import ABC, abstractmethod
import shapely
from dateutil import parser as date_parser
from pathlib import Path
import json

from .. import format
from ..format import format_value


class BaseDoc(object):
    def __init__(self):
        self._title = None
        self._description = None
        self._tags = None
        self._doi = None
        self._pid = None
        self._source = None
        self._related_identifier = None
        self._metadata_access = None
        self._creator = None
        self._publisher = None
        self._contributor = None
        self._publication_year = None
        self._rights = None
        self._open_access = None
        self._contact = None
        self._language = None
        self._resource_type = None
        self._format = None
        self._discipline = None

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
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, value):
        self._tags = format_value(value, type='string_words')

    @property
    def doi(self):
        return self._doi

    @doi.setter
    def doi(self, value):
        self._doi = format_value(value, type='url', one=True)

    @property
    def pid(self):
        return self._pid

    @pid.setter
    def pid(self, value):
        self._pid = format_value(value, type='url', one=True)

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, value):
        self._source = format_value(value, type='url', one=True)

    @property
    def related_identifier(self):
        return self._related_identifier

    @related_identifier.setter
    def related_identifier(self, value):
        self._related_identifier = format_value(value, type='url',)

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
        self._contributor = format_value(value)

    @property
    def publication_year(self):
        return self._publication_year

    @publication_year.setter
    def publication_year(self, value):
        self._publication_year = format_value(value, type='date_year')

    @property
    def rights(self):
        return self._rights

    @rights.setter
    def rights(self, value):
        self._rights = format_value(value)

    @property
    def open_access(self):
        return self._open_access or False

    @open_access.setter
    def open_access(self, value):
        self._open_access = format_value(value, type='boolean', one=True)

    @property
    def contact(self):
        return self._contact

    @contact.setter
    def contact(self, value):
        self._contact = format_value(value)

    @property
    def language(self):
        return self._language

    @language.setter
    def language(self, value):
        self._language = format_value(value)

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
    def discipline(self):
        return self._discipline or 'Various'

    @discipline.setter
    def discipline(self, value):
        self._discipline = format_value(value, one=True)


class GeoDoc(BaseDoc):
    def __init__(self):
        super().__init__()
        self._temporal_coverage_begin = None
        self._temporal_coverage_end = None
        self._geometry = None
        self._start_date = None
        self._end_date = None

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
        return self._geometry

    @geometry.setter
    def geometry(self, value):
        self._geometry = value

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
    def temporal_coverage_begin(self):
        return self._temporal_coverage_begin

    @temporal_coverage_begin.setter
    def temporal_coverage_begin(self, value):
        self._temporal_coverage_begin = format_value(value, type='date', one=True)

    @property
    def temporal_coverage_end(self):
        return self._temporal_coverage_end

    @temporal_coverage_end.setter
    def temporal_coverage_end(self, value):
        self._temporal_coverage_end = format_value(value, type='date', one=True)

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


class B2FDoc(GeoDoc):

    def __init__(self, filename, url=None, community=None, mdprefix=None):
        super().__init__()
        self.filename = filename
        self.url = url
        self.community = community
        self.mdprefix = mdprefix
        self._oai_set = None
        self._oai_identifier = None
        self._fulltext = None

    @property
    def name(self):
        return Path(self.filename).stem

    @property
    def fulltext(self):
        return self._fulltext

    @fulltext.setter
    def fulltext(self, value):
        self._fulltext = value

    @property
    def oai_set(self):
        return self._oai_set

    @oai_set.setter
    def oai_set(self, value):
        self._oai_set = format_value(value)

    @property
    def oai_identifier(self):
        return self._oai_identifier

    @oai_identifier.setter
    def oai_identifier(self, value):
        self._oai_identifier = format_value(value)
