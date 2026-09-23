

class AuthExpiredError(ConnectionError):

    def __str__(self):
        return "The authorization has been expired"


class BaseUnauthError(ConnectionError):

    def __str__(self):
        return "The base has not been authorized"


class ScaleDBAPIError(ConnectionError):

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code
