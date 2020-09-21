from .base import Community
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


def get_communities(cls=None):
    cls = cls or Community
    if len(cls.__subclasses__()) == 0:
        yield cls
    else:
        for subcls in cls.__subclasses__():
            yield from get_communities(subcls)


def community(identifier):
    logging.debug(f'community identifier={identifier}')
    for community in get_communities():
        if community.IDENTIFIER == identifier:
            return community
    raise CommunityNotSupported(f'Community not supported: {identifier}')
