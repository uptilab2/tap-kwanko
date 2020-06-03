#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests
from datetime import datetime


REQUIRED_CONFIG_KEYS = ["debut", "stats_or_sale", "authl", "authv"]
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
        stream_metadata = []
        key_properties = []

        if stream_id == "sale_reqann":
            replication_key = list(schema.properties.keys())[8]
        else:
            replication_key = list(schema.properties.keys())[0]

        if stream_id in ['stats_lisann_dim_1', 'stats_lisann_dim_2']:
            replication_method = "FULL_TABLE"
        else:
            replication_method = "INCREMENTAL"
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=replication_key,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=replication_method,
            )
        )
    return Catalog(streams)


def loop_continue(config, stream):
    if config['stats_or_sale'] == "/lisann.php" \
            and "stats_lisann" not in stream.tap_stream_id:

        print('Skipping %s' % stream.tap_stream_id)
        return True

    elif config['stats_or_sale'] == "/reqann.php" \
            and "sale_reqann" not in stream.tap_stream_id:

        print('Skipping %s' % stream.tap_stream_id)
        return True

    elif config['stats_or_sale'] != "/reqann.php":
        if stream.tap_stream_id == "stats_lisann_dim_1" \
                and config['dim'] != 1:

            print('Skipping %s' % stream.tap_stream_id)
            return True

        elif stream.tap_stream_id == "stats_lisann_dim_2" \
                and config['dim'] != 2:

            print('Skipping %s' % stream.tap_stream_id)
            return True

        elif stream.tap_stream_id == "stats_lisann_dim_3_4" \
                and config['dim'] != 3:

            if stream.tap_stream_id == "stats_lisann_dim_3_4" \
                    and config['dim'] != 4:
                print('Skipping %s' % stream.tap_stream_id)
                return True


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        if loop_continue(config, stream) is True:
            continue

        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        bookmark_column = stream.replication_key
        singer.write_schema(
            stream_name=stream.stream,
            schema=stream.stream,
            key_properties=stream.key_properties,
        )

        tap_data = get_data_from_API(config, state)

        for row in tap_data:

            keys = list(stream.schema.properties.keys())
            value = row.split(";")
            record_dict = {}

            for i in range(0, len(value)):
                record_dict[keys[i]] = value[i]

            singer.write_records(stream.tap_stream_id, [record_dict])

        bookmark_state(bookmark_column, config, tap_data, state)
    return


def bookmark_state(bookmark_column, config, tap_data, state):
    if bookmark_column == "date" and config['stats_or_sale'] == "/reqann.php":
        last_date = tap_data[-1].split(";")[8]
        new_state = singer.write_bookmark(state, "properties",
                                          "date_reqann", last_date)

        singer.write_state(new_state)

    elif bookmark_column == "idcamp" and config['dim'] == 1:
        last_date = datetime.now().isoformat(timespec='hours')[0:10]
        new_state = singer.write_bookmark(state, "properties",
                                          "date_lisann_dim_1", last_date)

        singer.write_state(new_state)

    elif bookmark_column == "idsite" and config['dim'] == 2:
        last_date = datetime.now().isoformat(timespec='hours')[0:10]
        new_state = singer.write_bookmark(state, "properties",
                                          "date_lisann_dim_2", last_date)

        singer.write_state(new_state)

    elif bookmark_column == "date" and config['dim'] == 3:
        last_date = tap_data[-1].split(";")[0]
        last_date = last_date[0:4] + "-" + last_date[4:6] + "-" + last_date[6:8]

        new_state = singer.write_bookmark(state, "properties", "date_lisann_dim_3", last_date)

        singer.write_state(new_state)

    elif bookmark_column == "date" and config['dim'] == 4:
        last_date = tap_data[-1].split(";")[0]
        last_date = last_date[0:4] + "-" + last_date[4:6] + "-01"

        new_state = singer.write_bookmark(state, "properties", "date_lisann_dim_4", last_date)

        singer.write_state(new_state)

    return


def get_data_from_API(config, state):

    try:
        state = state['value']['bookmarks']['properties']
        if config['stats_or_sale'] == "/reqann.php" \
                and state['date_reqann'] is not None:
            debut = state['date_reqann'][0:10]

        elif config['stats_or_sale'] == "/lisann.php" \
                and config['dim'] == 1 and state['date_lisann_dim_1'] is not None:

            debut = state['date_lisann_dim_1'][0:10]

        elif config['stats_or_sale'] == "/lisann.php" \
                and config['dim'] == 2 and state['date_lisann_dim_2'] is not None:

            debut = state['date_lisann_dim_2'][0:10]

        elif config['stats_or_sale'] == "/lisann.php" \
                and config['dim'] == 3 and state['date_lisann_dim_3'] is not None:

            debut = state['date_lisann_dim_3'][0:10]

        elif config['stats_or_sale'] == "/lisann.php" \
                and config['dim'] == 4 and state['date_lisann_dim_4'] is not None:

            debut = state['date_lisann_dim_4'][0:10]

        else:

            debut = config['debut']

    except KeyError:
        debut = config['debut']

    url = "https://stat.netaffiliation.com" + config['stats_or_sale']
    today = datetime.now().isoformat(timespec='hours')[0:10]
    if config['stats_or_sale'] == "/reqann.php":
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "debut": debut,
                                             "fin": today,
                                             "champs": config['champs_reqann']})
    elif config['stats_or_sale'] == "/lisann.php":
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "dim": config['dim'],
                                             "camp": config['camp'],
                                             "debut": debut,
                                             "fin": today,
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
