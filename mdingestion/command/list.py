import pandas as pd

from .base import Command
from ..community import repo, repos


class List(Command):

    def run(self, verbose=False, out=None):
        df = self.build_dataframe()
        if not verbose:
            df = df.loc[df.Productive == 'Yes']
        if out:
            df.to_csv(out)
        else:
            print(df)

    def build_dataframe(self):
        df = pd.DataFrame(columns=[
            'Repository',
            'Repository Title',
            'Group',
            'Group Title',
            'Identifier',
            'Productive',
            'Date',
            'Schema',
            'Service',
            'URL',
            'OAI Set',
            'MetadataPrefix',
            'Logo',
            'Description'])
        pd.set_option('display.max_rows', None)
        for identifier in repos('all'):
            com = repo(identifier)
            row = {
                'Repository': com.name,
                'Repository Title': com.TITLE,
                'Group': com.group,
                'Group Title': com.GROUP_TITLE,
                'Identifier': com.identifier,
                'Productive': 'Yes' if com.PRODUCTIVE else 'No',
                'Date': com.DATE if com.PRODUCTIVE else '',
                'Schema': com.SCHEMA,
                'Service': com.SERVICE_TYPE,
                'URL': com.URL,
                'OAI Set': com.OAI_SET,
                'MetadataPrefix': com.OAI_METADATA_PREFIX,
                'Logo': com.LOGO,
                'Description': com.DESCRIPTION,
            }
            df = pd.concat([df, pd.DataFrame(row, index=[0])], ignore_index=True)
        # df.set_index('Group', inplace=True)
        df = df.sort_values(by=['Repository', 'Group'])
        df_sorted = pd.DataFrame(
            data=df.values,
            columns=df.columns)
        return df_sorted
