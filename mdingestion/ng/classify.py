import os
import re
import json
from collections import OrderedDict
import Levenshtein as lvs
# import textdistance

from .format import format_value
from .util import remove_duplicates_from_list

import logging

CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'mapfiles')


def tokenize(text):
    tokens = []
    values = format_value(text, type='string_words')
    for value in values:
        _tokens = re.split(r'[;&\s]\s*', value)
        tokens.extend(_tokens)
        tokens.append(value)
    tokens = remove_duplicates_from_list(tokens)
    # words start with title case characters, all remaining cased characters have lower case
    tokens = [token.title() for token in tokens]
    return tokens


def similarity(string1, string2):
    return lvs.ratio(string1.lower(), string2.lower())
    # return textdistance.jaro_winkler.normalized_similarity(string1.lower(), string2.lower())


class Classify(object):
    def __init__(self):
        self._discipines = None

    def load_disciplines(self):
        discipines = {}
        fname = os.path.join(CFG_DIR, 'b2find_disciplines.json')
        with open(fname) as fp:
            doc = json.load(fp)['disciplines']
            for line in doc:
                line = re.split(r'#', line)
                if len(line) < 3:
                    logging.warning(f'Missing base element in dicipline array {line}')
                    continue
                discipline = line[2].strip()
                discipines[discipline] = line
        return discipines

    @property
    def discipines(self):
        if self._discipines is None:
            self._discipines = self.load_disciplines()
        return self._discipines

    def map_discipline(self, keywords):
        """
        Map keywords to B2Find disciplines.
        """
        matches = {}
        tokens = tokenize(keywords)
        for token in tokens:
            logging.debug(f'compare token {token}')
            max_ratio = 0.0
            best_match = ''
            for discipline in self.discipines:
                try:
                    ratio = similarity(token, discipline)
                except Exception as e:
                    logging.warning(f'{token} can not be compared to {discipline}: {e}')
                    continue
                if ratio > max_ratio:
                    max_ratio = ratio
                    best_match = discipline
                    logging.debug(f'token={token}, discipline={discipline}, ratio={ratio}, max_ratio={max_ratio}')
            logging.debug(f'Similarity ratio is {max_ratio} for token "{token}" and best match "{best_match}"')
            if max_ratio >= 0.9:
                logging.info(f'ratio >= 0.9. Keep best_match "{best_match}"')
                if best_match in matches:
                    if max_ratio > matches[best_match]:
                        matches[best_match] = max_ratio
                else:
                    matches[best_match] = max_ratio
        if matches:
            # sort by highest ratio
            sorted_matches = sorted(matches, key=lambda match: matches[match], reverse=True)
            result = (';'.join(sorted_matches), self.discipines[sorted_matches[0]])
        else:
            result = ('Various', [])
        return result
