class B2FError(Exception):
    message = "Unknown Error"

    def __init__(self, message=None):
        if message:
            self.message = message
        super().__init__(self.message)


class CommunityNotFound(B2FError):
    message = "Community not found in harvest list"


class HarvesterError(B2FError):
    message = "Harvester raised an error"


class HarvesterNotSupported(HarvesterError):
    message = "Harvester not supported"


class OAISetNotSupported(HarvesterError):
    message = "OAI set not supported by OAI service."


class UserInfo(Exception):
    pass
