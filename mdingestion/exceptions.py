class B2FError(Exception):
    message = "Unknown Error"

    def __init__(self, message=None):
        if message:
            self.message = message
        super().__init__(self.message)


class RepositoryNotSupported(B2FError):
    message = "Repository not supported"


class HarvesterError(B2FError):
    message = "Harvester raised an error"


class HarvesterNotSupported(HarvesterError):
    message = "Harvester not supported"


class OAISetNotSupported(HarvesterError):
    message = "OAI set not supported by OAI service."


class MappingError(B2FError):
    message = "Mapping raised an error"


class GeometryNotValid(MappingError):
    message = "Geometry could not be mapped"


class UserInfo(Exception):
    pass
