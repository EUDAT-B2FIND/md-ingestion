import json
from jsonpath_ng import parse as parse_jsonpath

from .base import DocParser
from .. import format


class JSONParser(DocParser):
    EXPR_CACHE = {}

    def get_parseexpr(self, name):
        if name in JSONParser.EXPR_CACHE:
            expr = JSONParser.EXPR_CACHE[name]
        else:
            expr = parse_jsonpath(name)
            JSONParser.EXPR_CACHE[name] = expr
        return expr

    def parse_doc(self):
        return json.load(open(self.filename))

    def find(self, name=None, **kwargs):
        expr = self.get_parseexpr(name)
        tags = expr.find(self.doc)
        return [format.format(tag.value, type=type) for tag in tags]

    @property
    def fulltext(self):
        """Pull all values from nested JSON."""
        arr = []

        def extract(obj, arr):
            """Recursively search for values in JSON tree."""
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        extract(v, arr)
                    else:
                        arr.append(v)
            elif isinstance(obj, list):
                for item in obj:
                    extract(item, arr)
            return arr

        results = extract(self.doc, arr)
        return results

    @classmethod
    def extension(cls):
        return '.json'
