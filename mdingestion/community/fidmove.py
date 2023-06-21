import json
import shapely
from .base import Repository
from ..service_types import SchemaType, ServiceType


class FIDmove(Repository):
    IDENTIFIER = 'fidmove'
    URL = 'https://data.fid-move.de/api/3'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.CKAN
    PRODUCTIVE = False
    DATE = ''
    REPOSITORY_ID = 're3data:r3d100014142'
    REPOSITORY_NAME = 'FID move'
    LOGO = ""
    LINK = 'https://data.fid-move.de/en/'
    DESCRIPTION = """The Research Data Repository of FID move is a long-term digital repository for open data in the field of transport and mobility research. All datasets are provided with an open licence and a persistent DataCite DOI (Digital Object Identifier). Both searching and archiving are free of charge.
The Specialised Information Service for Mobility and Transport Research (FID move) has been established by the Saxon State and University Library Dresden (SLUB) and the German National Library of Science and Technology (TIB – Leibniz Information Centre for Science and Technology) as part of the DFG funding programme "Specialised Information Services". The aim of FID move is the development and establishment of services and online tools in close consultation with the transport and mobility research community to support this community throughout the entire research cycle.
"""
