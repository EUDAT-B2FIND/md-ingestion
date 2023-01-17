import pandas as pd

from .base import Command
from ..community import repo, repos


class CronGen(Command):

    def run(self, name=None, productive=False, out=None):
        name = name or 'all'
       