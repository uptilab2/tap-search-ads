import os
import singer

logger = singer.get_logger()

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
        self.key_properties = [name+'Id']

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

    def write(self, metadata):
        self.write_schema()
        self.sync(metadata)
        self.write_state()

    def get_request_body(self, schema):
        payloads = {
            'reportScope':{
                'agencyId': self.config['agency_id'],
                'advertiserId': self.config['advertiser_id'],
                'engineAccountId': self.config['engineAccount_id']
            },
            'reportType': self.name,
            'columns': [{'columnName': column_name} for column_name in schema['properties']],
            'timeRange': {
                'startDate': self.config['start_date'][:10],
                'endDate': self.config['end_date'][:10],
            },
            'downloadFormat': 'CSV',
            'maxRowsPerFile': 100000000,
            'statisticsCurrency': 'agency'
        }
        return payloads

    def sync(self, mdata):
        logger.info(f'syncing {self.name}')
        schema = self.load_schema()
        data = self.client.get_data(self.get_request_body(schema))
        with singer.metrics.job_timer(job_type=f'list_{self.name}') as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:
                for d in data:
                    with singer.Transformer(integer_datetime_fmt="unix-seconds-integer-datetime-parsing") as transformer:
                        transformed_record = transformer.transform(data=d, schema=schema, metadata=singer.metadata.to_map(mdata))
                        singer.write_record(stream_name=self.name, time_extracted=singer.utils.now(), record=transformed_record)
                        counter.increment()

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