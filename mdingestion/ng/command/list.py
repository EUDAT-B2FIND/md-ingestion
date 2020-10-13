
from .base import Command
from ..community import community, communities


class List(Command):

    def run(self, name=None):
        name = name or 'all'
        for identifier in communities(name):
            com = community(identifier)
            print(f"{com.NAME}, {com.IDENTIFIER}, {com.URL}, {com.SCHEMA}, {com.SERVICE_TYPE}")
