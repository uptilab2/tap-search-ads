import singer
from tap-searchads360.client import GoogleSearchAdsClient

LOGGER = singer.get_logger()
SCOPE = "https://www.googleapis.com/auth/doubleclicksearch"
REQUIRED_CONFIG_KEYS = ['reports_id', 'client_id', 'client_secret', 'refresh_token']



@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    client = GoogleSearchAdsClient(
        args.config['client_id'],
        args.config['client_secret'],
        args.config['refresh_token']
    )


if __name__ == "__main__":
    main()

"""
Need to know:

** if reports is already generated, so get files from from report id and parsing data
** if not we need to request a reports and once it is ready get data getFile
    need reportScope: contains { agencyId, advertiserId, engineAccountId }
    https://developers.google.com/search-ads/v2/how-tos/reporting/asynchronous-requests?hl=fr
"""