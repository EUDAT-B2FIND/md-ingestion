from .ckan import CKANWriter
from .b2f import B2FWriter
from .legacy import LegacyWriter


def writer(format):
    if format == 'b2f':
        writer = B2FWriter()
    elif format == 'ckan':
        writer = CKANWriter()
    else:
        writer = LegacyWriter()
    return writer


__all__ = [
    CKANWriter,
    B2FWriter,
    LegacyWriter,
    writer,
]
