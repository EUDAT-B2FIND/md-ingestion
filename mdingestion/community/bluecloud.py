from shapely.geometry import shape
import json

from .base import Community
from ..service_types import SchemaType, ServiceType

from ..format import format_value


class Bluecloud(Community):
    NAME = 'bluecloud'
    IDENTIFIER = NAME
    URL = 'https://data.blue-cloud.org/api/collections'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.BC
    PRODUCTIVE = False
