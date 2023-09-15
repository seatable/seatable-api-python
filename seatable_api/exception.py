

class AuthExpiredError(ConnectionError):

    def __str__(self):
        return "The authorization has been expired"


class BaseUnAuthError(ConnectionError):

    def __str__(self):
        return "The base has not been authorized"
