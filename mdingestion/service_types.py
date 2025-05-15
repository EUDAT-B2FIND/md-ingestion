from enum import Enum


class SchemaType(Enum):
    DublinCore = 0
    DataCite = 1
    ISO19139 = 2
    FGDC = 3
    FF = 4
    DDI25 = 5
    Eudatcore = 6
    OLAC = 7
    JSON = 100


class ServiceType(Enum):
    OAI = 0
    CSW = 2
    HERBADROP = 100
    ArcGIS = 200
    BC = 300
    Dataverse = 400
    CKAN = 500
    DataCite = 600
