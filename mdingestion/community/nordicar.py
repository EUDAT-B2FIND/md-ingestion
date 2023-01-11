from shapely.geometry import shape
import pandas as pd
import os
import copy
from .base import Repository


CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'etc', 'Community')
FNAME = os.path.join(CFG_DIR, 'NORDICAR_MappingKeywords.csv')
TL = pd.read_csv(FNAME, sep=';', encoding='ISO-8859-1')


class BaseNordicar(Repository):
    GROUP = 'nordicar'
    GROUP_TITLE = 'Nordic Archaeology'
    PRODUCTIVE = True
    DATE = '2021-11-15'
    DESCRIPTION = 'This Community consists of records from Danish and Norwegian Data Providers. Askeladden is a Norwegian database system for managing cultural heritage monuments and sites. Askeladden is owned and managed by Riksantikvaren. The Directorate for Cultural Heritage (Riksantikvaren) was established in 1912. They are responsible for the management of cultural heritage, cultural environments and cultural landscape of historic importance. The Agency for Culture and Palaces was formed on 1 January 2016 in a merger of the Danish Agency for Culture and the Agency for Palaces and Cultural Properties. The agency provides advice to the Danish minister of culture and is involved in setting and achieving the governments cultural policy goals.'
    LOGO = ''

    def keywords_append(self, doc):
        keywords = copy.copy(doc.keywords)
        for keyword in doc.keywords:
            result = TL.loc[TL[self.IDENTIFIER] == keyword]
            if result.values.any():
                found = result.values[0].tolist()
                found = [val for val in found if not pd.isnull(val)]
                keywords.extend(found)
        return keywords
