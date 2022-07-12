from .base import Community
from ..service_types import SchemaType, ServiceType
from shapely.geometry import shape

import json
import pandas as pd
import os
import copy


class BasePanosc(Community):
    NAME = 'panosc'
    TITLE = 'PaNOSC'
    PRODUCTIVE = False

class ESRFDatacite(BasePanosc):
    NAME = 'esrf'
    IDENTIFIER = 'esrf'
    URL = 'https://icatplus.esrf.fr/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None

    def update(self, doc):
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.publication_year = self.publicationyear(doc)
#        doc.temporal_coverage = <dates><date dateType="Collected">2022-04-26T07:30:00Z/2022-04-27T06:00:00Z</date>
#        doc.keywords = self.keywords(doc)
        doc.publisher = 'ESRF (European Synchrotron Radiation Facility)'

#    def keywords(self, doc):
#        keywords = doc.keywords
#        keywords.append('PaN')#
#        return keywords

#    def publicationyear (self, doc):
#       if not <publicationyear>:
#       if datetype: "Available": nimm den Wert, sonst datetype:"Accepted"
