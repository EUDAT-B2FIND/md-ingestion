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
        df = pd.DataFrame(columns=['Community', 'Sub Community', 'Productive', 'Date', 'Schema', 'Service', 'URL', 'OAI Set'])
        pd.set_option('display.max_rows', None)
        for identifier in communities(name):
            com = community(identifier)
            df = df.append({
                'Community': com.NAME,
                'Sub Community': com.IDENTIFIER,
                'Productive': com.PRODUCTIVE,
                'Date': com.DATE if com.PRODUCTIVE else '',
                'Schema': com.SCHEMA,
                'Service': com.SERVICE_TYPE,
                'URL': com.URL,
                'OAI Set': com.OAI_SET},
                ignore_index=True)
        # df.set_index('Group', inplace=True)
        df = df.sort_values(by=['Community', 'Sub Community'])
        df_sorted = pd.DataFrame(
            data=df.values,
            columns=df.columns)
        return df_sorted
