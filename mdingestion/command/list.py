import pandas as pd
import json

from .base import Command
from ..community import repo, repos


class List(Command):

    def run(self, verbose=False, stat=False, out=None):
        df = self.build_dataframe()
        if stat:
            self.build_stat(df, out)
            return
        if not verbose:
            df = df.loc[df.Productive == 'Yes']
            df = df[['Identifier', 'Repository Title', 'Schema', 'Service', 'URL', 'OAI Set', 'MetadataPrefix']]
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

    def build_stat(self, df, out):
        df = df.loc[df.Productive == 'Yes']
        df = df[['Identifier', 'Repository', 'Schema', 'Service', 'URL', 'MetadataPrefix']]
        # Ensure all values in the DataFrame are strings
        df = df.astype(str)
        # Generate value counts with total count
        summary_with_totals = {}
        for col in df.columns:
            value_counts = df[col].value_counts().to_dict()
            total_count = len(df[col])
            unique_count = len(value_counts)  # Total number of unique values (keys)
            value_counts["Total Unique Keys"] = str(unique_count)
            value_counts["Total"] = total_count
            # Convert all values to strings
            summary_with_totals[col] = {k: str(v) for k, v in value_counts.items()}

        # Convert to JSON
        summary_json = json.dumps(summary_with_totals, indent=4)

        if out:
            # Save the JSON output to a file
            with open(out, "w") as json_file:
                json.dump(summary_with_totals, json_file, indent=4)
        else:
            # Print the JSON output
            print("JSON Output:")
            print(summary_json)
