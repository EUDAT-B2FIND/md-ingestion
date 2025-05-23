from .base import Repository
from ..service_types import SchemaType, ServiceType


class OpenebenchEudatcore(Repository):
    IDENTIFIER = 'openebench'
    TITLE = 'OpenEBench'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '591815be-a801-47c5-80e2-0a39f95c7def'
    PRODUCTIVE = True
    DATE = '2023-05-10'
    REPOSITORY_ID = 're3data:r3d100011394'
    REPOSITORY_NAME = 'B2SHARE'
    CRON_DAILY = False
    LOGO = "https://legacy.openebench.bsc.es/assets/img/opeb_logo.gif"
    LINK = "https://openebench.bsc.es", "https://elixir-europe.org/platforms/tools"
    DESCRIPTION = """OpenEBench is the ELIXIR benchmarking and technical monitoring platform for bioinformatics tools, web servers and workflows. OpenEBench is part of the ELIXIR Tools platform and its development is led by the Barcelona Supercomputing Center (BSC) in collaboration with partners within ELIXIR and beyond. Within the ELIXIR project, OpenEBench is being developed under the Tools Platform at the Work Package 2 (WP2: Benchmarking). All OpenEBench components have been designed and implemented following the recommendations made by the ELIXIR tools platform e.g. making code available in public repositories from day 1; are available as software containers, and use workflow managers promoted by ELIXIR. Next figure illustrates the interconnection of OpenEBench to other ELIXIR tools platforms systems and platforms and beyond."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
