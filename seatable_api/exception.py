

class AuthExpiredError(ConnectionError):

    def __str__(self):
        return "The authorization has been expired"

class UserAuthMissingError(Exception):

    def __str__(self):
        return 'User authorization is missing'
