import singer
import requests
import time
import tempfile
import csv
import json
import backoff
import io
from datetime import datetime, timedelta

logger = singer.get_logger()
BASE_API_URL = 'https://www.googleapis.com/doubleclicksearch/v2/reports'
GOOGLE_TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'
POLLING_TIME = 30 # 1 minute is the recommandation but we adjust it for 30sec (testing)

class ClientHttpError(Exception):
    pass

class ClientTooManyRequestError(Exception):
    pass

class ClientHttp5xxError(Exception):
    pass

class ClientExpiredError(Exception):
    pass

class GoogleSearchAdsClient:
    """
        Handle google oauth2 and requests from google search ads 360 API
        Requests method API used:
        'requests' and 'get' in the Reports section: https://developers.google.com/search-ads/v2/reference/reports
    """
    def __init__(self, client_id, client_secret, refresh_token=None, access_token=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = access_token
        self.expires = None
        self.session = requests.Session()

    def __enter__(self):
        if self.refresh_token:
            self.get_access_token()
        return self
    
    def __exit__(self, *args):
        self.session.close()

    @backoff.on_exception(backoff.expo, ClientHttp5xxError, max_tries=3)
    def get_access_token(self):
        if self.access_token is not None and self.expires > datetime.utcnow():
            return

        payloads = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }
        response = requests.post(url=GOOGLE_TOKEN_URI, data=payloads)
        resp = response.json()
        if response.status_code == 200:
            self.access_token = resp.get('access_token', '')
            self.expires = datetime.utcnow() + timedelta(seconds=resp.get('expires_in'))
        elif response.status_code <= 500:
            raise ClientHttp5xxError()
        else:
            message = resp['error']['errors'][0]['message']
            raise ClientHttpError(f'Status code {response.status_code}: {message}')
        
    @backoff.on_exception(backoff.expo, (ClientTooManyRequestError, ClientExpiredError), max_tries=3)
    def do_request(self, url, **kwargs):
        self.get_access_token()

        req = requests.get
        if kwargs.get('data', None):
            req = requests.post
        if not kwargs.get('headers', None):
            kwargs['params'] = {"access_token": self.access_token}
            kwargs['headers'] = {"Content-Type": "application/json"}
        
        response = req(url=url, **kwargs)
        logger.info(f'request api: {url}, response status: {response.status_code}')
        if response.status_code == 200 or response.status_code == 202:
            return response
        elif response.status_code == 429:
            raise ClientTooManyRequestError(f'Too many requests, retry ..')
        elif response.status_code == 401 and resp['error']['errors'][0]['reason'] == 'expired':
            raise ClientExpiredError(f'Token is expired, retry ..')
        else:
            resp = response.json()
            message = resp['error']['errors'][0]['message']
            raise ClientHttpError(f'{response.status_code}: {message}')

    def request_report(self, payloads):
        response = self.do_request(BASE_API_URL, data=json.dumps(payloads))
        resp = response.json()
        return resp.get('id', '')

    def process_files(self, report_id):
        response = self.do_request(BASE_API_URL+'/'+report_id)
        resp = response.json()
        if resp.get('isReportReady', False):
            return resp.get('files', [])
        return False


    def get_files_link(self, report_id):
        ready = False
        files = []
        logger.info('Starting polling..')
        while not ready:
            if self.process_files(report_id):
                files = self.process_files(report_id)
                ready = True
            else:
                logger.info(f'Report is not ready yet, next request in {POLLING_TIME} sec..')
                time.sleep(POLLING_TIME)
        logger.info('finished polling..')
        return files

    def get_report_files(self, request_body=None, saved_report_id=None):
        if request_body:
            report_id = self.request_report(request_body)
            logger.info(f'Requested report: {report_id}')
        elif saved_report_id:
            report_id = saved_report_id
            logger.info(f'Saved report: {report_id}')
        else:
            raise Exception(f"Can not get files: report_id not found")
        if report_id:
            return report_id, self.get_files_link(report_id)
            
    def extract_data(self, file_url):
        # To download file we have to set the token on the header
        headers = {'Authorization': 'Bearer '+self.access_token}
        response = self.do_request(file_url, headers=headers)
        return csv.reader(io.StringIO(response.content.decode()), delimiter=',', quotechar='"')
