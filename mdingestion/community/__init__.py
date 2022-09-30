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


def repo(identifier):
    logging.debug(f'repository identifier={identifier}')
    for _repo in get_repositories():
        if _repo.IDENTIFIER == identifier:
            return _repo()
    raise RepositoryNotSupported(f'Repository not supported: {identifier}')


def repos(name):
    logging.debug(f'rpository name={name}')
    repo_list = []
    for _repo in get_repositories():
        if name == 'all':
            repo_list.append(_repo.IDENTIFIER)
        elif _repo.NAME == name:
            repo_list.append(_repo.IDENTIFIER)
        elif _repo.IDENTIFIER == name:
            repo_list.append(_repo.IDENTIFIER)
    if not repo_list:
        raise RepositoryNotSupported(f'Repository not supported: {name}')
    return repo_list
