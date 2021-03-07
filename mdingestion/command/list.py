import pandas as pd

from .base import Command
from ..community import community, communities


class List(Command):

    def run(self, name=None, groups=False, all=False, summary=False):
        name = name or 'all'
        df = self.build_dataframe(name)
        if all:
            print(df)
        elif groups:
            print(df.Group.unique())
        elif summary:
            print(df.nunique())
        else:
            print(df.Community.unique())

    def build_dataframe(self, name):
        df = pd.DataFrame(columns=['Community', 'Group', 'Schema', 'Service', 'URL'])
        pd.set_option('display.max_rows', None)
        for identifier in communities(name):
            com = community(identifier)
            df = df.append({
                'Community': com.NAME,
                'Group': com.IDENTIFIER,
                'Schema': com.SCHEMA,
                'Service': com.SERVICE_TYPE,
                'URL': com.URL},
                ignore_index=True)
        return df
