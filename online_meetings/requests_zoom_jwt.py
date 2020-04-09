"""
    Class for managing tokens, expirations and generations.
    Currently just handles JWT for Zoom
"""
from datetime import datetime, timedelta
import requests
from requests.auth import AuthBase
import jwt


class ZoomJWT(AuthBase):

    @property
    def token(self) -> str:
        """Check if the token is expired, if so generate a new token and return the token either way.
        
        :return: Returning the token value as a string
        :rtype: str
        """
        # If token is close to expiring just re-create
        if datetime.utcnow() > self.expiration - timedelta(seconds=30):
            self.token = self.create_token()

        return self._token

    @token.setter
    def token(self, value: str):
        self._token = value
    
    @property
    def expiration(self) -> datetime:
        return self._expiration

    @expiration.setter
    def expiration(self, value: datetime):
        self._expiration = value

    def __init__(self, api_key: str, api_secret: str, alg="HS256", exp_seconds: int = 60):
        """ Generates a JWT Token for Zoom
        :param api_key: api key to use
        :type api_key: str
        :param api_secret: secret to use
        :type api_secret: str
        :param exp_seconds: How long to expire the token.
        :type exp_seconds: int
        :return: token as a string
        :rtype: str
        """
        
        # If the expiration time is less than 60, this really won't work well, just set to 60
        if exp_seconds < 60:
            exp_seconds = 60
        self.exp_seconds = exp_seconds
        self.api_key = api_key
        self.api_secret = api_secret
        self.alg = alg
        self.token = self.create_token()

    def create_token(self):
        """Creates a token using JWT and sets the expiration using values in this class
        """
        self.expiration = datetime.utcnow() + timedelta(seconds=self.exp_seconds)
        token = jwt.encode(
                {'iss': self.api_key,
                 'exp': self.expiration
                 }, self.api_secret, algorithm=self.alg)
        # Need to decode https://github.com/jpadilla/pyjwt/issues/391
        return token.decode('utf-8')

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        """ For requests library to call
        :param request: Request from requests
        :type request: requests.PreparedRequest
        :return: Request with an Authorization header
        :rtype: requests.PreparedRequest
        """
        request.headers['Authorization'] = f"Bearer {self.token}"
        return request
