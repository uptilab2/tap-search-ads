import os
import singer
import hashlib
import json
from copy import copy
from datetime import datetime, timedelta


logger = singer.get_logger()


LIMIT_DAYS_PER_REPORT = 365

SPECIFIC_REPLICATION_KEYS = [
    {'conversion': 'conversionDate'},
    {'visit': 'visitDate'}
]

AVAILABLE_SEGMENT = {
    'account': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag'],
    'ad': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag'],
    'advertiser': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag'],
    'adGroup': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag',
                'sitelinkDisplayText', 'sitelinkDescription1', 'sitelinkDescription2', 'sitelinkLandingPageUrl', 'sitelinkClickserverUrl', 'locationBusinessName', 'locationCategory', 'locationDetails', 'locationFilter', 'callPhoneNumber', 'callCountryCode', 'callIsTracked', 'callCallOnly',
                'callConversionTracker', 'callConversionTrackerId', 'appId', 'appStore', 'feedItemId', 'feedId', 'feedType'],
    'adGroupTarget': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag'],
    'bidStrategy': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd'],
    'campaign': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag',
                 'sitelinkDisplayText', 'sitelinkDescription1', 'sitelinkDescription2', 'sitelinkLandingPageUrl', 'sitelinkClickserverUrl', 'locationBusinessName', 'locationCategory', 'locationDetails', 'locationFilter', 'callPhoneNumber', 'callCountryCode', 'callIsTracked', 'callCallOnly',
                 'callConversionTracker', 'callConversionTrackerId', 'appId', 'appStore', 'feedItemId', 'feedId', 'feedType'],
    'campaignTarget': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag'],
    'conversion': [],
    'feedItem': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag'],
    'floodlightActivity': [],
    'keyword': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag',
                'ad', 'adId', 'isUnattributedAd', 'adHeadline', 'adHeadline2', 'adHeadline3', 'adDescription1', 'adDescription2', 'adDisplayUrl', 'adLandingPage', 'adType', 'adPromotionLine'],
    'negativeAdGroupKeyword': [],
    'negativeAdGroupTarget': [],
    'negativeCampaignKeyword': [],
    'negativeCampaignTarget': [],
    'paidAndOrganic': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'campaign', 'campaignId', 'adGroup', 'adGroupId', 'keywordId', 'keywordMatchType', 'keywordText'],
    'productAdvertised': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag', 'accountId', 'campaignId', 'adGroupId'],
    'productGroup': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment'],
    'productLeadAndCrossSell': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag', 'accountId', 'campaignId', 'adGroupId'],
    'productTarget': ['date', 'monthStart', 'monthEnd', 'quarterStart', 'quarterEnd', 'weekStart', 'weekEnd', 'yearStart', 'yearEnd', 'deviceSegment', 'floodlightGroup', 'floodlightGroupId', 'floodlightGroupTag', 'floodlightActivity', 'floodlightActivityId', 'floodlightActivityTag'],
    'visit': []
}

AVAILABLE_STREAMS = [
    'account',
    'ad',
    'adGroup',
    'adGroupTarget',
    'advertiser',
    'bidStrategy',
    'campaign',
    'campaignTarget',
    'conversion',
    'feedItem',
    'floodlightActivity',
    'keyword',
    'negativeAdGroupKeyword',
    'negativeAdGroupTarget',
    'negativeCampaignKeyword',
    'negativeCampaignTarget',
    'paidAndOrganic',
    'productAdvertised',
    'productGroup',
    'productLeadAndCrossSell',
    'productTarget',
    'visit'
]

# helpers
def converting_value(value, type):
    try:
        if 'format' in type:
            if type['format'] == 'date-time':
                return datetime.strptime(value, '%Y-%m-%d').strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if type['type'][1] == 'string':
            return str(value)
        elif type['type'][1] == 'boolean':
            return bool(value)
        elif type['type'][1] == 'number':
            return float(value)
        elif type['type'][1] == 'integer':
            return int(value)
    except:
        return str(value)

def parsing_filter_value(value, check_type = int):
    # try casting type from jsonconfig
    try:
        return check_type(value)
    except:
        return parsing_filter_value(value, check_type=float) if check_type is int else str(value)

class DateRangeError(Exception):
    pass

class SegmentValueError(Exception):
    pass

class DataIsMissingError(Exception):
    pass

class Stream:
    replication_method = 'INCREMENTAL'
    forced_replication_method = 'INCREMENTAL'
    valid_replication_keys = []

    def __init__(self, name, client=None, config=None, catalog_stream=None, state=None):
        if name not in AVAILABLE_STREAMS:
            raise f"The stream {name} doesn't exists"
        self.name = name
        self.client = client
        self.config = config
        self.catalog_stream = catalog_stream
        self.state = state

    def get_abs_path(self, path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def load_schema(self):
        schema_path = self.get_abs_path('schemas')
        return singer.utils.load_json('{}/{}.json'.format(schema_path, self.name))

    def write_schema(self, columns=None):
        schema = self.load_schema()
        if columns:
            selected_properties = {prop[0]: prop[1] for prop in schema['properties'].items() if prop[0] in columns}
            schema['properties'] = selected_properties
        return singer.write_schema(stream_name=self.name, schema=schema, key_properties=self.key_properties)

    def write_state(self):
        return singer.write_state(self.state)

    
class SearchAdsStream(Stream):
    valid_replication_keys = ['lastModifiedTimestamp']
    replication_key = 'lastModifiedTimestamp'
    data = []
    fields = []
    filters = []
    advertisers = []

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.key_properties = [name+'Id']

        # set replication_method
        if 'full_table_replication' in self.config and self.config['full_table_replication']:
            self.replication_method = 'FULL_TABLE'
            self.forced_replication_method = 'FULL_TABLE'

        # setting up custom_report, filters and replication_key
        if 'custom_report' in self.config:
            custom_reports = [custom_report for custom_report in self.config['custom_report'] if name == custom_report['name']]
        self.set_options(self.config, custom_reports[0] if custom_reports else None)

        # set replicat_method for reports that have specific properties and can't be change
        if name in SPECIFIC_REPLICATION_KEYS:
            self.replication_key = SPECIFIC_REPLICATION_KEYS[name]
            self.valid_replication_keys = SPECIFIC_REPLICATION_KEYS[name]

    def set_options(self, config, custom_report=None):
        if custom_report:
            # set fields
            if 'columns' in custom_report and custom_report['columns']:
                self.fields = custom_report['columns']

            # set filters
            if 'filters' in custom_report and custom_report['filters']:
                self.filters = custom_report['filters']

        # set replication key
        if 'replication_key' in config:
            schema = self.load_schema()
            # check if exists
            if any([prop for prop in schema['properties'] if prop == config['replication_key']]):
                if self.fields and config['replication_key'] not in self.fields:
                    raise Exception('Replication key must be in the report field selection. Please check your config file')
                self.replication_key = config['replication_key']
                self.valid_replication_keys = [config['replication_key']]
            else:
                logger.info(f"Custom replication key not found. default is set: {self.replication_key}")

    def selected_properties(self, metadata, fields=None):
        """
            This function does:
            - select only the properties in report list selection in config.
            - if no selection fields "selected:false" property is apply to all segment.
        """
        mdata, columns, selected_fields = metadata, [], []
        # add selected false to properties we don't want
        schema = self.load_schema()
        if fields:
            selected_fields = fields
        else:
            # select all except segments
            selected_fields = [prop for prop in schema['properties'] if prop not in AVAILABLE_SEGMENT[self.name] or prop == self.replication_key]
         
        for field in mdata:
            if field['breadcrumb']:
                columns.append(field['breadcrumb'][1])
                if field['breadcrumb'][1] not in selected_fields:
                    field['metadata'].update(selected = 'false')
                    columns.pop(-1)
        return columns, mdata

    def write(self, metadata):
        columns, metadata = self.selected_properties(metadata, fields=self.fields)
        self.write_schema(columns)
        self.sync(columns, metadata)

    def get_date_range_request(self, start_date, end_date):
        #check start_date and end_date offset
        if start_date >= end_date:
            raise DateRangeError(f"start_date should be at least 1 days ago")

        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        delta = end - start
        days = delta.days
        dates = []
        end_range = end
        while days > LIMIT_DAYS_PER_REPORT:
            end_range = start + timedelta(days=LIMIT_DAYS_PER_REPORT)
            dates.append(
                (f'{start.year}-{start.month:02}-{start.day:02}T00:00:00Z',
                 f'{end_range.year}-{end_range.month:02}-{end_range.day:02}T00:00:00Z')
            )
            start = end_range
            delta = end - start
            days = delta.days
        else:
            dates.append(
                (f'{start.year}-{start.month:02}-{start.day:02}T00:00:00Z',
                f'{end.year}-{end.month:02}-{end.day:02}T00:00:00Z')
            )
        return dates

    def request_body(self, agency_id, advertiser_id, columns, start_date, end_date, filters=None):
        payloads = {
            'reportScope':{
                'agencyId': agency_id,
            },
            'reportType': self.name,
            'columns': [{'columnName': column_name} for column_name in columns],
            'timeRange': {
                'startDate': start_date,
                'endDate': end_date
            },
            'downloadFormat': 'CSV',
            'maxRowsPerFile': 100000000,  # max rows value
            'statisticsCurrency': self.config['currency'] if 'currency' in self.config and self.config['currency'] in ('agency', 'advertiser', 'account', 'usd') else 'usd' # noqa
        }
        if self.name != 'advertiser': # need the specific list here noqa
            if 'engineAccount_id' in self.config and self.config['engineAccount_id']:
                payloads['reportScope']['engineAccountId'] = self.config['engineAccount_id']
        if advertiser_id:
            payloads['reportScope']['advertiserId'] = advertiser_id
        if filters:
            payloads['filters'] = [{
                "column": {"columnName" : f['field']},
                "operator": f['operator'],
                "values": [parsing_filter_value(f['value'])],
            }
            for f in filters]
        exit
        return payloads


    def get_bookmark(self, advertiser_id):
        bookmark = singer.get_bookmark(self.state, self.name, advertiser_id, {})
        if not bookmark:
            bookmark['date'] = self.config.get('start_date')
        else:
            if 'offset_start_date' in self.config and self.config.get('offset_start_date', 0):
                start = datetime.strptime(bookmark['date'][:10], '%Y-%m-%d') - timedelta(days=int(self.config.get('offset_start_date')))
                logger.info(start)
                bookmark['date'] = f'{start.year}-{start.month:02}-{start.day:02}T00:00:00Z'
                logger.info(bookmark['date'])
        return bookmark

    def sync(self, columns, mdata):
        logger.info(f'syncing {self.name}')
        schema = self.load_schema()
        advertiser_ids = self.config['advertiser_id'] if  isinstance(self.config['advertiser_id'], list) else [self.config['advertiser_id']]
        for advertiser_id in advertiser_ids:
            bookmark = self.get_bookmark(advertiser_id)

            yesterday = datetime.now() - timedelta(days=1)
            start_date = bookmark['date'][:10]
            end_date = self.config['end_date'][:10] if 'end_date' in self.config and self.config['end_date'] else str(yesterday.strftime('%Y-%m-%d'))
            max_date = bookmark['date'][:10]

            # get date ranges split into multiple dates ranges of 365 days interval
            date_ranges = self.get_date_range_request(start_date, end_date)
            for start_date, end_date in date_ranges:
                bookmark = self.get_bookmark(advertiser_id)
                report_id = ''
                files = []
                logger.info(f'Request a report from {start_date} to {end_date}')
                request_body = self.request_body(self.config['agency_id'], advertiser_id, columns, start_date[:10], end_date[:10], filters=self.filters)
                logger.info(request_body)
                # bookmark report_id, if something wrong happen use it to get files again
                if bookmark.get('report_id', None)\
                and not bookmark.get('complete', False)\
                and bookmark.get('offset') < bookmark.get('file_count')\
                and bookmark['extract_date'][:10] == str(datetime.now())[:10]:
                    report_id, files = self.client.get_report_files(saved_report_id=bookmark.get('report_id'))

                if not report_id and not files:
                    report_id, files = self.client.get_report_files(request_body)
                    bookmark.update({
                        'report_id': report_id,
                        'file_count': len(files),
                        'offset': 0,
                        'extract_date': str(datetime.now())[:10],
                        'complete': False
                    })
                logger.info(f'Report {report_id} contain {len(files)} files')
                
                new_bookmark = copy(bookmark)
                for count, file in enumerate(files):
                    if bookmark['offset'] > count:
                        continue
                        
                    data = self.client.extract_data(file.get('url'))
                    logger.info(f'Writing records for {self.name} from file : '+file.get('url'))
                    with singer.metrics.job_timer(job_type=f'list_{self.name}') as timer:
                        with singer.metrics.record_counter(endpoint=self.name) as counter:
                            for line_count, line in enumerate(data):
                                if line_count == 0:
                                    # remove first line
                                    continue
                                dict = {key: (converting_value(value, schema['properties'][key]) if value else None) for (key, value) in zip(columns, line)}
                                max_date = max(max_date, dict.get(self.replication_key, ''))
                                if (self.replication_method == 'INCREMENTAL' and dict.get(self.replication_key, '')[:10] > start_date[:10]) or self.replication_method == 'FULL_TABLE':
                                    singer.write_record(stream_name=self.name, time_extracted=singer.utils.now(), record=dict)
                                    counter.increment()
                    # save between each file for retry purpose
                    new_bookmark['offset'] += 1
                    self.state = singer.write_bookmark(self.state, self.name, advertiser_id, new_bookmark)
                    self.write_state()
                # when everything is done save the date, we can't order by column only with synchronous report
                new_bookmark['date'] = max_date
                new_bookmark['complete'] = True
                self.state = singer.write_bookmark(self.state, self.name, advertiser_id, new_bookmark)
                self.write_state()

