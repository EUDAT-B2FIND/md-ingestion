import os
import re
import json
import Levenshtein as lvs
# import textdistance
import networkx as nx

from .format import format_value

import logging

CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'etc')


def similarity(string1, string2):
    return lvs.ratio(string1.lower(), string2.lower())
    # return textdistance.jaro_winkler.normalized_similarity(string1.lower(), string2.lower())


def tokenize(text):
    tokens = set()
    values = format_value(text, type='string_words')
    tokens.update(values)
    for value in values:
        _tokens = re.split(r'[;&\s]\s*', value)
        tokens.update(_tokens)
    tokens = [token.lower() for token in tokens]
    tokens.sort()
    return tokens


class Classify(object):
    def __init__(self):
        self._disc_graph = None
        self._discipines = None

    def load_disciplines(self):
        fname = os.path.join(CFG_DIR, 'b2find_disciplines.json')
        with open(fname) as fp:
            doc = json.load(fp)
            # build graph
            self._disc_graph = nx.DiGraph()
            for value in doc["disciplines"]:
                parts = value.split("#")
                if len(parts) != 3:
                    continue
                nodes = [node.strip() for node in parts]
                start = nodes[1]
                end = nodes[2]
                if start != end:
                    self._disc_graph.add_edge(start, end)
        return self._disc_graph.nodes

    @property
    def discipines(self):
        if self._discipines is None:
            self._discipines = self.load_disciplines()
        return self._discipines

    def map_discipline(self, text, default=None):
        """
        Map text to B2Find disciplines.
        """
        default = default or "Other"
        matches = set()
        tokens = tokenize(text)
        for token in tokens:
            for discipline in self.discipines:
                try:
                    ratio = similarity(token, discipline)
                except Exception:
                    logging.warning(f'{token} can not be compared to {discipline}')
                    continue
                if ratio >= 0.9:
                    matches.add(discipline)
                    matches.update(nx.ancestors(self._disc_graph, discipline))
        result = list(matches)
        if not result:
            result = [default]
        else:
            result.sort()
        return result
