import os
import singer
from copy import copy
from datetime import datetime, timedelta

logger = singer.get_logger()


SPECIFIC_REPLICATION_KEYS = [
    {'conversion': 'conversionDate'},
    {'visit': 'visitDate'}
]    
AVAILABLE_SEGMENT = [
    'date',
    'monthStart',
    'monthEnd',
    'quarterStart',
    'quarterEnd',
    'weekStart',
    'weekEnd',
    'yearStart',
    'yearEnd',
    'deviceSegment',
    'floodlightGroup',
    'floodlightGroupId',
    'floodlightGroupTag',
    'floodlightActivity',
    'floodlightActivityId',
    'floodlightActivityTag',
    'sitelinkDisplayText',
    'sitelinkDescription1',
    'sitelinkDescription2',
    'sitelinkLandingPageUrl',
    'sitelinkClickserverUrl',
    'locationBusinessName',
    'locationCategory',
    'locationDetails',
    'locationFilter',
    'callPhoneNumber',
    'callCountryCode',
    'callIsTracked',
    'callCallOnly',
    'callConversionTracker',
    'callConversionTrackerId',
    'appId',
    'appStore',
    'feedItemId',
    'feedId',
    'feedType',
    'accountId',
    'campaign',
    'adGroup',
    'keywordId',
    'keywordMatchType',
    'keywordText',
    'campaignId',
    'adGroupId',
    'ad',
    'adId',
    'isUnattributedAd',
    'adHeadline',
    'adHeadline2',
    'adHeadline3',
    'adDescription1',
    'adDescription2',
    'adDisplayUrl',
    'adLandingPage',
    'adType',
    'adPromotionLine'
]
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

    def write_schema(self):
        schema = self.load_schema()
        return singer.write_schema(stream_name=self.name, schema=schema, key_properties=self.key_properties)

    def write_state(self):
        return singer.write_state(self.state)

    
class SearchAdsStream(Stream):
    valid_replication_keys = ['lastModifiedTimestamp']
    replication_key = 'lastModifiedTimestamp'
    data = []
    fields = []
    filters = []
    
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.key_properties = [name+'Id']
        
        # set replicat_method for reports that have specific properties
        if name in SPECIFIC_REPLICATION_KEYS:
            self.replication_key = SPECIFIC_REPLICATION_KEYS[name]
            self.valid_replication_keys = SPECIFIC_REPLICATION_KEYS[name]

        # setting up custom_report if exists
        if 'custom_report' in self.config and name in self.config['custom_report']:
            self.set_custom_report(self.config['custom_report'][name])


    def set_custom_report(self, custom_report):
        # set fields
        if 'columns' in custom_report:
            self.fields = custom_report['columns']
        # set filters
        if 'filters' in custom_report:
            self.filters = custom_report['filters']

        # set replication key
        if 'replication_key' in custom_report:
            schema = self.load_schema()
            # check if exists
            if any([prop for prop in schema['properties'] if prop == custom_report['replication_key']]):
                if 'columns' in custom_report:
                    if custom_report['replication_key'] not in custom_report['columns']:
                        raise Exception('Replication key must be in the report field selection. Please check your config file')
                self.replication_key = custom_report['replication_key']
                self.valid_replication_keys = [custom_report['replication_key']]
            else:
                raise Exception(f"Replication key not found. Please check your config file")



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
            selected_fields = [prop for prop in schema['properties'] if prop not in AVAILABLE_SEGMENT]
         
        for field in mdata:
            if field['breadcrumb']:
                columns.append(field['breadcrumb'][1])
                if field['breadcrumb'][1] not in selected_fields:
                    field['metadata'].update(selected = 'false')
                    columns.pop(-1)
        return columns, mdata

    def write(self, metadata):
        self.write_schema()
        self.sync(metadata)
        self.write_state()

    def request_body(self, columns, filters=None):
        yesterday = datetime.now() - timedelta(days=1)
        default_end_date = yesterday.strftime('%Y-%m-%d')
        if self.config['start_date'][:10] > str(yesterday) and 'end_date' not in self.config:
            raise DateRangeError(f"start_date should be at least 1 days ago")

        payloads = {
            'reportScope':{
                'agencyId': self.config['agency_id'],
                'advertiserId': self.config['advertiser_id']
            },
            'reportType': self.name,
            'columns': [{'columnName': column_name} for column_name in columns],
            'timeRange': {
                'startDate': self.config['start_date'][:10],
                'endDate': default_end_date
            },
            'downloadFormat': 'CSV',
            'maxRowsPerFile': 1000000, # min rows value
            'statisticsCurrency': 'agency'
        }
        if 'end_date' in self.config:
            payloads['timeRange']['endDate'] = self.config['end_date'][:10]
        # advertiser report is the only one who don't need engineAccount_id
        if self.name != 'advertiser':
            payloads['reportScope']['engineAccountId']: self.config['engineAccount_id']
        if filters:
            payloads['filters'] = [{
                "column": {"columnName" : f['field']},
                "operator": f['operator'],
                "values": [f['value']],
            }
            for f in filters]
        return payloads


    def get_bookmark(self):
        bookmark = singer.get_bookmark(state=self.state, tap_stream_id=self.name, key=self.replication_key)
        if bookmark is None:
            bookmark = self.config.get('start_date')
        return bookmark

    def sync(self, mdata):
        logger.info(f'syncing {self.name}')
        columns, metadata = self.selected_properties(mdata, fields=self.fields)
        data = self.client.get_data(self.request_body(columns, filters=self.filters))
        schema = self.load_schema()
        bookmark = self.get_bookmark()
        new_bookmark = bookmark
        with singer.metrics.job_timer(job_type=f'list_{self.name}') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for d in data:
                    with singer.Transformer(integer_datetime_fmt="unix-seconds-integer-datetime-parsing") as transformer:
                        transformed_record = transformer.transform(data=d, schema=schema, metadata=singer.metadata.to_map(metadata))
                        new_bookmark = max(new_bookmark, transformed_record.get(self.replication_key))
                        if (self.replication_method == 'INCREMENTAL' and transformed_record.get(self.replication_key) > bookmark) or self.replication_method == 'FULL_TABLE':
                            singer.write_record(stream_name=self.name, time_extracted=singer.utils.now(), record=transformed_record)
                            counter.increment()
        self.state = singer.write_bookmark(state=self.state, tap_stream_id=self.name, key=self.replication_key, val=new_bookmark)

