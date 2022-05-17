import pandas as pd

from .base import Command
from ..community import community, communities


class List(Command):

    def run(self, name=None, summary=False, productive=False, out=None):
        name = name or 'all'
        df = self.build_dataframe(name)
        if productive:
            df = df.loc[df.Productive == productive]
        if out:
            df.to_csv(out)
        elif summary:
            print(df.nunique().to_string())
        else:
            print(df)

    def build_dataframe(self, name):
        df = pd.DataFrame(columns=[
            'Community', 
            'Group', 
            'Identifier', 
            'Productive', 
            'Date', 
            'Schema', 
            'Service', 
            'URL', 
            'OAI Set',
            'Logo',
            'Description'])
        pd.set_option('display.max_rows', None)
        for identifier in communities(name):
            com = community(identifier)
            row = {
                'Community': com.NAME,
                'Group': com.GROUP,
                'Identifier': com.IDENTIFIER,
                'Productive': com.PRODUCTIVE,
                'Date': com.DATE if com.PRODUCTIVE else '',
                'Schema': com.SCHEMA,
                'Service': com.SERVICE_TYPE,
                'URL': com.URL,
                'OAI Set': com.OAI_SET,
                'Logo': com.LOGO,
                'Description': com.DESCRIPTION,
            }
            df = pd.concat([df, pd.DataFrame(row, index=[0])], ignore_index=True)
        # df.set_index('Group', inplace=True)
        df = df.sort_values(by=['Community', 'Group'])
        df_sorted = pd.DataFrame(
            data=df.values,
            columns=df.columns)
        return df_sorted
