from .base import Community
from ..exceptions import RepositoryNotSupported

import logging

"""Import all modules that exist in the current directory."""
# Ref https://stackoverflow.com/a/60861023/
from importlib import import_module
from pathlib import Path

REPOSITORIES = None

for f in Path(__file__).parent.glob("*.py"):
    module_name = f.stem
    if (not module_name.startswith("_")) and (module_name not in globals()):
        import_module(f".{module_name}", __package__)
    del f, module_name
del import_module, Path


def _repositories(cls=None):
    cls = cls or Community
    if len(cls.__subclasses__()) == 0:
        yield cls
    else:
        for subcls in cls.__subclasses__():
            yield from _repositories(subcls)


def get_repositories():
    global REPOSITORIES
    if not REPOSITORIES:
        REPOSITORIES = []
        for com in _repositories():
            REPOSITORIES.append(com)
    return REPOSITORIES


def community(identifier):
    logging.debug(f'community identifier={identifier}')
    for community in get_repositories():
        if community.IDENTIFIER == identifier:
            return community()
    raise RepositoryNotSupported(f'Repository not supported: {identifier}')


def communities(name):
    logging.debug(f'community name={name}')
    com_list = []
    for community in get_repositories():
        if name == 'all':
            com_list.append(community.IDENTIFIER)
        elif community.NAME == name:
            com_list.append(community.IDENTIFIER)
        elif community.IDENTIFIER == name:
            com_list.append(community.IDENTIFIER)
    if not com_list:
        raise RepositoryNotSupported(f'Repository not supported: {name}')
    return com_list
