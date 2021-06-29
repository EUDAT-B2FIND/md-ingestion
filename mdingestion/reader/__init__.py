from .eudatcore import EudatcoreReader
from .datacite import DataCiteReader
from .dc import DublinCoreReader
from .iso19139 import ISO19139Reader
from .json import JSONReader
from .fgdc import FGDCReader
from .ff import FFReader

from ..service_types import SchemaType
from ..sniffer import sniffer


def build_reader(reader_type=None, service_type=None):
    if reader_type == SchemaType.Eudatcore:
        reader = EudatcoreReader()
    elif reader_type == SchemaType.DataCite:
        reader = DataCiteReader()
    elif reader_type == SchemaType.ISO19139:
        reader = ISO19139Reader()
    elif reader_type == SchemaType.FGDC:
        reader = FGDCReader()
    elif reader_type == SchemaType.FF:
        reader = FFReader()
    elif reader_type == SchemaType.JSON:
        reader = JSONReader()
    else:
        reader = DublinCoreReader()
    reader.SNIFFER = sniffer(service_type)
    return reader


__all__ = [
    EudatcoreReader,
    DataCiteReader,
    DublinCoreReader,
    ISO19139Reader,
    JSONReader,
    build_reader,
    FGDCReader,
    FFReader,
]
