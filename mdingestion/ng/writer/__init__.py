from .ckan import CKANWriter
from .b2f import B2FWriter


def writer(format):
    if format == 'b2f':
        writer = B2FWriter()
    else:
        writer = CKANWriter()
    return writer


__all__ = [
    CKANWriter,
    B2FWriter,
    LegacyWriter,
    writer,
]
