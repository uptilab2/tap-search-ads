import json
import singer
import sys
import threading
from .client import GoogleSearchAdsClient
from .streams import SearchAdsStream, AVAILABLE_STREAMS

logger = singer.get_logger()
REQUIRED_CONFIG_KEYS = ['client_id', 'client_secret', 'refresh_token', 'start_date', 'agency_id', 'advertiser_id', 'engineAccount_id']

def get_catalog(streams):
    catalog = {}
    catalog['streams'] = []
    for stream in streams:
        schema = stream.load_schema()
        catalog_entry = {
            'stream': stream.name,
            'tap_stream_id': stream.name,
            'schema': schema,
            'metadata': singer.metadata.get_standard_metadata(schema=schema,
                                                              key_properties=stream.key_properties,
                                                              valid_replication_keys=stream.valid_replication_keys,
                                                              replication_method=stream.replication_method)
        }
        catalog['streams'].append(catalog_entry)
    return catalog

def discover(config=None):
    logger.info('Starting discover ..')
    streams = [SearchAdsStream(stream_name, config=config) for stream_name in AVAILABLE_STREAMS]
    catalog = get_catalog(streams)
    logger.info('Finished discover ..')
    return json.dump(catalog, sys.stdout, indent=2)

def sync(client, config, catalog, state):
    logger.info('Starting Sync..')
    for catalog_entry in catalog.get_selected_streams(state):
        stream = SearchAdsStream(name=catalog_entry.stream, client=client, config=config, catalog_stream=catalog_entry.stream, state=state)
        logger.info('Syncing stream: %s', catalog_entry.stream)
        # Each stream can be very long in time, because google needs to generate CSV files then we downloads and parse them.
        try:
            thread = threading.Thread(target=stream.write, args=(catalog_entry.metadata,))
            thread.start()
        except Exception as e:
            raise e

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    client = GoogleSearchAdsClient(
        args.config['client_id'],
        args.config['client_secret'],
        args.config['refresh_token']
    )
    
    with client:
        if args.discover:
            discover(config=args.config)
        else:
            sync(client=client, config=args.config, catalog=args.catalog, state=args.state)
    
if __name__ == "__main__":
    main()
