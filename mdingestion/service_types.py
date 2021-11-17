from enum import Enum


class SchemaType(Enum):
    DublinCore = 0
    DataCite = 1
    ISO19139 = 2
    FGDC = 3
    FF = 4
    DDI25 = 5
    Eudatcore = 6
    JSON = 100


class ServiceType(Enum):
    OAI = 0
    CSW = 1
    HERBADROP = 100
    ArcGIS = 200
    BC = 300
