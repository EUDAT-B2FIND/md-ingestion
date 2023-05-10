from .base import Repository
from ..service_types import SchemaType, ServiceType


class WhateverinEudatcore(Repository):
    IDENTIFIER = 'example'
    # if possible please avoid too long names and '-' or '+' because we never know how it looks like in the browser then, must be the same name as the mapfile, e.g. example.py
    TITLE = 'ExAmple'
    # the Name of the Repository on the Web GUI
    GROUP = 'b2share'
    # like this because that attaches a tag to each record of this example community
    URL = 'https://b2share.eudat.eu/api/oai2d'
    # caution: different for b2share_fzj!
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = 'veeeery-important-string-here'
    # Whatever Set from CSC / FZJ
    PRODUCTIVE = False
    # as long as it is not on productive machine...
    DATE = '2023-01-23'
    # will be done by B2FIND when it is productive
    REPOSITORY_ID = ''
    # very important because this information is transferred to OpenAIRE; allowed values only IDs from re3data or fairsharing, example: REPOSITORY_ID = 're3data:r3d100013171'
    REPOSITORY_NAME = ''
    # same as above, example: REPOSITORY_NAME = 'DaRUS'
    CRON_DAILY = False
    LINK = ""
    # important for creating the Repository CKAN Web GUI
    LOGO = ""
    # important for creating the Repository CKAN Web GUI
    DESCRIPTION = """"""
    # important for creating the Repository CKAN Web GUI

    def update(self, doc):
        # all update methods are default for *if not*, so if the information is given we take it but if there is no information we have a default
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
            # because publisher = mandatory element in B2FIND
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
            # because publication_year = mandatory element in B2FIND
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
            # because otherwise records don´t go to OpenAIRE...
        if not doc.discipline:
            doc.discipline = 'Social/Natural Sciences'
            # because it´s good for the search, only for thematic communities
        if not doc.contact:
            doc.contact = 'somecrazyemail@coolrepo.eu, www.anyhelpdesk.eu'
            # because that could be useful, don´t use when there is none
