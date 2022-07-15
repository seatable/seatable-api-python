

class AuthExpiredError(ConnectionError):

    def __str__(self):
        return "The authorization has been expired"
