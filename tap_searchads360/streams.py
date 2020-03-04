import os
import singer
from datetime import datetime, timedelta

logger = singer.get_logger()

class DateRangeError(Exception):
    pass

class Stream:
    replication_method = 'FULL_TABLE'
    forced_replication_method = 'FULL_TABLE'
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
    replication_method = 'INCREMENTAL'
    forced_replication_method = 'INCREMENTAL'
    valid_replication_keys = ['lastModifiedTimestamp']
    replication_key = 'lastModifiedTimestamp'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.key_properties = [name+'Id']
        # setting up optional config
        if 'metrics_date_segment' in self.config and self.config['metrics_date_segment']:
            schema = self.load_schema()
            if any([prop for prop in schema['properties'] if prop == 'date']):
                self.replication_key = 'date'
                self.valid_replication_keys = ['date']
        # set replicat_method for reports that have specific properties
        if name in SPECIFIC_REPLICATION_KEYS:
            self.replication_key = name+'Date'
            self.valid_replication_keys = [name+'Date']

    def selected_properties(self, metadata):
        # add selected false to properties we don't want
        mdata, columns = metadata, []
        for field in mdata:
            if field['breadcrumb']:
                columns.append(field['breadcrumb'][1])
                if 'metrics_date_segment' in self.config and not self.config['metrics_date_segment']:
                    if 'date' in field['breadcrumb']:
                        field['metadata'].update(selected = 'false')
                        columns.pop(-1)
        return columns, mdata

    def write(self, metadata):
        self.write_schema()
        self.sync(metadata)
        self.write_state()
        logger.info(f'Finished sync stream: {self.name}')

    def request_data(self, columns):
        yesterday = datetime.now() - timedelta(days=1)
        default_end_date = yesterday.strftime('%Y-%m-%d')
        if self.config['start_date'][:10] > str(yesterday) and 'end_date' not in self.config:
            raise DateRangeError(f"start_date should be at least 1 days ago")

        payloads = {
            'reportScope':{
                'agencyId': self.config['agency_id'],
                'advertiserId': self.config['advertiser_id'],
                'engineAccountId': self.config['engineAccount_id']
            },
            'reportType': self.name,
            'columns': [{'columnName': column_name} for column_name in columns],
            'timeRange': {
                'startDate': self.config['start_date'][:10],
                'endDate': default_end_date
            },
            'downloadFormat': 'CSV',
            'maxRowsPerFile': 100000000,
            'statisticsCurrency': 'agency'
        }
        if 'end_date' in self.config:
            payloads['timeRange']['endDate'] = self.config['end_date'][:10]
        return payloads

    def get_bookmark(self):
        bookmark = singer.get_bookmark(state=self.state, tap_stream_id=self.name, key=self.replication_key)
        if bookmark is None:
            bookmark = self.config.get('start_date')
        return bookmark

    def sync(self, mdata):
        logger.info(f'syncing {self.name}')
        schema = self.load_schema()
        columns, metadata = self.selected_properties(mdata)
        data = self.client.get_data(self.request_data(columns))
        if data:
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

SPECIFIC_REPLICATION_KEYS = [
    'conversion',
    'visit'
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