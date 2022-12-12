from .base import Repository
from ..exceptions import RepositoryNotSupported

import logging

"""Import all modules that exist in the current directory."""
# Ref https://stackoverflow.com/a/60861023/
from importlib import import_module
from pathlib import Path

CACHED_ORGS = {
    Repository: [],
}


for f in Path(__file__).parent.glob("*.py"):
    module_name = f.stem
    if (not module_name.startswith("_")) and (module_name not in globals()):
        import_module(f".{module_name}", __package__)
    del f, module_name
del import_module, Path


def _find_orgs(cls=None):
    cls = cls or Repository
    if len(cls.__subclasses__()) == 0:
        yield cls
    else:
        for subcls in cls.__subclasses__():
            yield from _find_orgs(subcls)

def _cached_orgs(cls=None):
    global CACHED_ORGS
    cls = cls or Repository
    if not CACHED_ORGS.get(cls):
        for org in _find_orgs(cls):
            CACHED_ORGS[cls].append(org)
    return CACHED_ORGS[cls]


def repo(identifier):
    # lookup repo by identifier and return an instance if found.
    logging.debug(f'repository identifier={identifier}')
    for org in _cached_orgs():
        if org.IDENTIFIER == identifier:
            return org()
    raise RepositoryNotSupported(f'Repository not supported: {identifier}')


def _orgs(name=None, cls=None):
    cls = cls or Repository
    name = name or 'all'
    org_list = []
    for _org in _cached_orgs(cls):
        if name == 'all':
            org_list.append(_org.IDENTIFIER)
        elif _org.IDENTIFIER == name:
            org_list.append(_org.IDENTIFIER)
        elif _org.IDENTIFIER == name:
            org_list.append(_org.IDENTIFIER)
        elif _org.GROUP == name:
            org_list.append(_org.IDENTIFIER)
    if not org_list:
        raise RepositoryNotSupported(f'Repository not supported: {name}')
    return org_list

def repos(name=None):
    return _orgs(name, cls=Repository)
