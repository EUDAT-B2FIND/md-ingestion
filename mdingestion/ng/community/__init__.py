from ..reader.base import Reader
from ..exceptions import CommunityNotSupported

import logging

"""Import all modules that exist in the current directory."""
# Ref https://stackoverflow.com/a/60861023/
from importlib import import_module
from pathlib import Path

for f in Path(__file__).parent.glob("*.py"):
    module_name = f.stem
    if (not module_name.startswith("_")) and (module_name not in globals()):
        import_module(f".{module_name}", __package__)
    del f, module_name
del import_module, Path


def get_readers(cls=None):
    cls = cls or Reader
    if len(cls.__subclasses__()) == 0:
        yield cls
    else:
        for subcls in cls.__subclasses__():
            yield from get_readers(subcls)


def reader(community, mdprefix):
    logging.debug(f'community={community}, mdprefix={mdprefix}')
    for reader in get_readers():
        if reader.NAME == f'{community}-{mdprefix}':
            return reader
    raise CommunityNotSupported(f'Community not supported: {community}-{mdprefix}')
