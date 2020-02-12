import singer
import requests

BASE_URL = 'https://www.googleapis.com'
GOOGLE_TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'
AUTH_URL = 'https://accounts.google.com/o/oauth2/auth'
SCOPE = 'https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdoubleclicksearch'
LOGGER = singer.get_logger()

class GoogleSearchAdsClient():
    """
        Handle google oauth2 and requests
    """
    def __init__(self, client_id, client_secret, refresh_token=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = None
        self.expires = None
        self.session = requests.Session()

    def __enter__(self):
        if self.refresh_token:
            self.get_access_token()
        else:
            self.login()
        return self
    
    def __exit__(self):
        self.session.close()

    def get_access_token(self):        
        payloads = {
            'grant_type': 'refresh_token',
            'client_id': self.__client_id,
            'client_secret': self.__client_secret,
            'refresh_token': self.__refresh_token
        }
        response = requests.post()

    def login(self):
        params = {
            'scope': SCOPE,
            'response_type': code,
            'client_id': self.client_id
        }
        response = requests.get(params=params)

        code = response.get('code', '')

        params_auth = {
            'code': code,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'authorization_code',
        }

        response_auth = requests.get(params=params_auth)

        json_response = json.loads(response_auth)

        if 'refresh_token' in json_response:
            self.refresh_token = json_response.get('refresh_token', '')
            # TODO WRITE IN JSON CONFIG
        else:
            # TODO Handle nope nope
            pass
    
    def requests(self):
        # TODO request maybe only files on reports
        pass