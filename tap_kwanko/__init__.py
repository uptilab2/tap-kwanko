#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests


REQUIRED_CONFIG_KEYS = ["debut", "fin", "stats_or_sale", "authl", "authv"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []

    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        if config['stats_or_sale'] == "/lisann.php" \
                and "stats_lisann" not in stream.tap_stream_id:

            print('Skipping %s' % stream.tap_stream_id)
            continue

        elif config['stats_or_sale'] == "/reqann.php" \
                and "sale_reqann" not in stream.tap_stream_id:

            print('Skipping %s' % stream.tap_stream_id)
            continue

        elif config['stats_or_sale'] != "/reqann.php":
            if stream.tap_stream_id == "stats_lisann_dim_1" \
                    and config['dim'] != 1:

                print('Skipping %s' % stream.tap_stream_id)
                continue

            elif stream.tap_stream_id == "stats_lisann_dim_2" \
                    and config['dim'] != 2:

                print('Skipping %s' % stream.tap_stream_id)
                continue

            elif stream.tap_stream_id == "stats_lisann_dim_3_4" \
                    and config['dim'] != 3:

                if stream.tap_stream_id == "stats_lisann_dim_3_4" \
                        and config['dim'] != 4:

                    print('Skipping %s' % stream.tap_stream_id)
                    continue

        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        bookmark_column = stream.replication_key
        # TODO: indicate whether data is sorted ascending on bookmark value
        is_sorted = True
        singer.write_schema(
            stream_name=stream.stream,
            schema=stream.stream,
            key_properties=stream.key_properties,
        )

        tap_data = get_data_from_API(config)
        max_bookmark = None

        for row in tap_data:
            # TODO: place type conversions or transformations here
            # write one or more rows to the stream:

            keys = list(stream.schema.properties.keys())
            value = row.split(";")
            record_dict = {}

            for i in range(0, len(value)):
                record_dict[keys[i]] = value[i]

            singer.write_records(stream.tap_stream_id, [record_dict])

            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


def get_data_from_API(config):
    url = "https://stat.netaffiliation.com" + config['stats_or_sale']
    if config['stats_or_sale'] == "/reqann.php":
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "debut": config['debut'],
                                             "fin": config['fin'],
                                             "champs": config['champs_reqann']})
    elif config['stats_or_sale'] == "/lisann.php":
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "dim": config['dim'],
                                             "camp": config['camp'],
                                             "debut": config['debut'],
                                             "fin": config['fin'],
                                             "per": config['per'],
                                             "champs": config['champs_lisann'],
                                             "site": config['site']})

    print("Result : %s" % response.text.splitlines()[0])

    if "OK" in response.text.splitlines()[0]:
        # skip 1st line telling how long the result is
        response = response.text.splitlines()[1:]
    else:
        print("Error on getting data from Kwanko (get_data_from_API())")
        return
    return response


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
