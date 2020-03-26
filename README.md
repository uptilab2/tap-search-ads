# tap search-ads

- (https://singer.io) tap that produces JSON-formatted data
following the [Singer](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the [Search ads 360 v2 API](https://developers.google.com/search-ads)
- Extracts the following type of reports from search ads:
    - [account](https://developers.google.com/search-ads/v2/report-types/account)
    - [ad](https://developers.google.com/search-ads/v2/report-types/ad)
    - [advertiser](https://developers.google.com/search-ads/v2/report-types/advertiser)
    - [adGroup](https://developers.google.com/search-ads/v2/report-types/adGroup)
    - [adGroupTarget](https://developers.google.com/search-ads/v2/report-types/adGroupTarget)
    - [bidStrategy](https://developers.google.com/search-ads/v2/report-types/bidStrategy)
    - [campaign](https://developers.google.com/search-ads/v2/report-types/campaign)
    - [campaignTarget](https://developers.google.com/search-ads/v2/report-types/campaignTarget)
    - [conversion](https://developers.google.com/search-ads/v2/report-types/conversion)
    - [feedItem](https://developers.google.com/search-ads/v2/report-types/feedItem)
    - [floodlightActivity](https://developers.google.com/search-ads/v2/report-types/floodlightActivity)
    - [keyword](https://developers.google.com/search-ads/v2/report-types/keyword)
    - [negativeAdGroupKeyword](https://developers.google.com/search-ads/v2/report-types/negativeAdGroupKeyword)
    - [negativeAdGroupTarget](https://developers.google.com/search-ads/v2/report-types/negativeAdGroupTarget)
    - [negativeCampaignKeyword](https://developers.google.com/search-ads/v2/report-types/negativeCampaignKeyword)
    - [negativeCampaignTarget](https://developers.google.com/search-ads/v2/report-types/negativeCampaignTarget)
    - [paidAndOrganic](https://developers.google.com/search-ads/v2/report-types/paidAndOrganic)
    - [productAdvertised](https://developers.google.com/search-ads/v2/report-types/productAdvertised)
    - [productGroup](https://developers.google.com/search-ads/v2/report-types/productGroup)
    - [productLeadAndCrossSell](https://developers.google.com/search-ads/v2/report-types/productLeadAndCrossSell)
    - [productTarget](https://developers.google.com/search-ads/v2/report-types/productTarget)
    - [visit](https://developers.google.com/search-ads/v2/report-types/visit)
- Outputs the schema for each report

## Quick start

1. Install

We recommend using a virtualenv:

```bash
> virtualenv -p python3 venv
> source venv/bin/activate
> pip install .
```
 
2. Setup your application and create your config file

The Google Search ads 360 Setup & Authentication Google Doc provides instructions show how to configure the Google Cloud API credentials to enable Google Search ads 360 APIs, configure Google Cloud to authorize/verify your domain ownership, generate an API key (client_id, client_secret), authenticate and generate a refresh_token, and prepare your tap config.json with the necessary parameters.

Enable Googe Search ads API and Authorization Scope: https://www.googleapis.com/auth/doubleclicksearch

Tap config.json parameters:
- client_id: identifies your application
- client_secret: authenticates your application
- refresh_token: generates an access token to authorize your session
- agency_id: unique identifier of your agency
- advertiser_id: unique identifier of your advertiser
- engineAccount_id: unique identifier of the account in the external engine account
- start_date: Inclusive date in YYYY-MM-DD format
- end_date: Inclusive date in YYYY-MM-DD format (optional: if is not added in config, end_date will be the current day)
- replication_key: set the replication_key (default:'lastModifiedTimestamp' except for report conversion = 'conversionDate' and visit = 'visitDate').

- custom_report: choose your columns for each type of report (see example below): 
    - name: The report name.
    - columns: Dict of columns you want to select for each type of report.
        /!\ WARNING
        Becareful, all segments fields are not selected by default, please refer to the documentation: https://developers.google.com/search-ads/v2/report-types and choose one segment per report.
    - filters: list of filters you want to use (see example below):
    
```
...
    "custom_report": [
        {
            "name": "campaign",
            "columns": [
                "status",
                "agency",
                "advertiser",
                ...
                "date"
            ],
            "filters": [
                {
                    "field": "status",
                    "operator": "equals",
                    "value": "ACTIVE"
                }
            ]

        }
    ]
...
```



3. Run the tap in discovery mode to get catalog.json file

```bash
tap-searchads360 --config config.json --discover > catalog.json
```

4. In the catalog.json file, select the streams to sync

Each stream in the catalog.json file has a "schema" entry.  To select a stream to sync, add `"selected": true` to that stream's "schema" entry. For example, to sync the campaign stream:
```
...
"stream": "campaign",
    "tap_stream_id": "campaign",
    "schema": {
    "selected": true,
    "type": [
        "null",
        "object"
    ],
    "additionalProperties": false,
    "properties": {
        "status": {
        "type": [
            "null",
            "string"
        ]
        },
...
```

5. Run the application

`tap-searchads360` can be run with:

```bash
tap-searchads360 --config config.json --catalog catalog.json
```
