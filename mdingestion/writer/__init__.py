from .ckan import CKANWriter
from .b2f import B2FWriter
from .skg import SkgWriter


def writer(format):
    if format == 'b2f':
        writer = B2FWriter()
    elif format == 'skg':
        writer = SkgWriter()
    else:
        writer = CKANWriter()
    return writer


__all__ = [
    CKANWriter,
    B2FWriter,
    SkgWriter,
    writer,
]
