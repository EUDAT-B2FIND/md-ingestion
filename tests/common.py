import os
import json

from mdingestion.mapping import Mapper as B2FMapper

import logging
LOGGER = logging.getLogger('tests')
LOGGER.setLevel(logging.DEBUG)

TESTS_HOME = os.path.abspath(os.path.dirname(__file__))
TESTDATA_DIR = os.path.join(TESTS_HOME, "testdata")


class Output(object):
    @property
    def logger(self):
        return LOGGER

    @property
    def verbose(self):
        return 3


class Mapper(object):
    def __init__(self, community, schema):
        self.community = community
        self.schema = schema
        self.mapper = B2FMapper(Output(), TESTDATA_DIR, None)

    def map(self):
        request = [self.community, 'https://www.envidat.ch/oai', 'ListRecords', self.schema]
        self.mapper.map(request)

    def load_result(self, filename, set_name=None):
        set_name = set_name or 'SET_1'
        result = json.load(open(
            os.path.join(
                TESTDATA_DIR,
                f"{self.community}-{self.schema}",
                f"{set_name}",
                "json",
                f"{filename}")))
        return result
