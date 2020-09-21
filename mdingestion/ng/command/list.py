
from .base import Command
from ..community import get_communities


class List(Command):

    def run(self):
        for community in get_communities():
            print(f"name={community.NAME}, identifier={community.IDENTIFIER}")
