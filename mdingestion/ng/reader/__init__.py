from .datacite import DataCiteReader
from .dc import DublinCoreReader
from .iso19139 import ISO19139Reader
from .json import JSONReader
from .base import SchemaType
from .fgdc import FGDCReader


def build_reader(reader_type=None):
    if reader_type == SchemaType.DataCite:
        reader = DataCiteReader()
    elif reader_type == SchemaType.ISO19139:
        reader = ISO19139Reader()
    elif reader_type == SchemaType.FGDC:
        reader = FGDCReader()
    elif reader_type == SchemaType.JSON:
        reader = JSONReader()
    else:
        reader = DublinCoreReader()
    return reader


__all__ = [
    DataCiteReader,
    DublinCoreReader,
    ISO19139Reader,
    JSONReader,
    SchemaType,
    build_reader,
    FGDCReader,
]
