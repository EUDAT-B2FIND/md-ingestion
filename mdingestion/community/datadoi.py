from .base import Repository
from ..service_types import SchemaType, ServiceType


class DatadoiDublincore(Repository):
    IDENTIFIER = 'datadoi'
    TITLE = 'DataDOI'
    URL = 'https://datadoi.ee/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = False
    DATE = '2023-06-13'
    CRON_DAILY = False
    REPOSITORY_ID = 're3data:r3d100012333'
    REPOSITORY_NAME = 'DataDOI'
    LINK = 'https://datadoi.ee/'
    LOGO = "https://utlib.ut.ee/sites/default/files/library_logo_eng_1.png"
    DESCRIPTION = """The mission of DataDOI is to offer preservation and dissemination of Estonian research data from various fields. The main goal is to ensure that research data produced by Estonian researchers is findable, accessible, interoperable and reusable. Through preserving open research data DataDOI enriches academic quality and collaboration, supports innovative developments and supports overall use of scientific materials. DataDOI is an institutional research data repository managed by University of Tartu Library. DataDOI gathers all fields of research data and stands for encouraging open science and FAIR (Findable, Accessible, Interoperable, Reusable) principles. DataDOI is made for long-term preservation of research data. Each dataset is given a DOI (Digital Object Identifier) through DataCite Estonia Concortium."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'University of Tartu Library'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        doc.doi = self.find_doi('dc:identifier')
        doc.pid = self.find_pid('dc:identifier')
        doc.source = self.find_source('dc:identifier')
