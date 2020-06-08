#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests
from datetime import datetime


REQUIRED_CONFIG_KEYS = ["debut", "authl", "authv"]
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

        replication_key = "date"
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


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):

        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        bookmark_column = stream.replication_key
        schema = stream.schema.to_dict()

        singer.write_schema(
            stream_name=stream.stream,
            schema=schema,
            key_properties=stream.key_properties,
        )

        id_list = set()
        name_list = set()

        if "stats_by_campain" in stream.tap_stream_id:
            tap_data = get_data_from_API(config, state, "stats_by_campain")

            for row in tap_data:
                value = row.split(";")
                id_value = value[0]
                name_value = value[1]

                id_list.add(id_value)
                name_list.add(name_value)
                continue

            id_list = list(id_list)
            name_list = list(name_list)

            for i in range(0, len(id_list)):
                tap_data = get_data_from_API_by_id(config, state, stream.tap_stream_id, id_list[i], "campain")
                for row in tap_data:

                    keys = list(stream.schema.properties.keys())
                    value = row.split(";")

                    record_dict = {}
                    record_dict['idcamp'] = id_list[i]
                    record_dict['nomcamp'] = name_list[i]

                    for j in range(0, len(value)):
                        if j == 0 : # init date and skip idcamp and nomcamp
                            record_dict[keys[0]] = value[0]
                        else:
                            record_dict[keys[j+2]] = value[j]
                    singer.write_records(stream.tap_stream_id, [record_dict])

        elif "stats_by_site" in stream.tap_stream_id:
            tap_data = get_data_from_API(config, state, "stats_by_site")

            for row in tap_data:
                value = row.split(";")
                id_value = value[0]
                name_value = value[1]

                id_list.add(id_value)
                name_list.add(name_value)
                continue

            id_list = list(id_list)
            name_list = list(name_list)

            for i in range(0, len(id_list)):
                tap_data = get_data_from_API_by_id(config, state, stream.tap_stream_id, id_list[i], "site")
                for row in tap_data:

                    keys = list(stream.schema.properties.keys())
                    value = row.split(";")

                    record_dict = {}
                    record_dict['idsite'] = id_list[i]
                    record_dict['nomsite'] = name_list[i]

                    for j in range(0, len(value)):
                        if j == 0 : # init date and skip idsite and nomsite
                            record_dict[keys[0]] = value[0]
                        else:
                            record_dict[keys[j+2]] = value[j]
                    singer.write_records(stream.tap_stream_id, [record_dict])

        else:
            tap_data = get_data_from_API(config, state, stream.tap_stream_id)

            for row in tap_data:

                keys = list(stream.schema.properties.keys())
                value = row.split(";")
                record_dict = {}
                for i in range(0, len(value)):
                    record_dict[keys[i]] = value[i]

                singer.write_records(stream.tap_stream_id, [record_dict])

        bookmark_state(bookmark_column, stream.tap_stream_id, tap_data, config, state)
    return


def bookmark_state(bookmark_column, tap_stream_id, tap_data, config, state):
    if bookmark_column == "date" and "sale" in tap_stream_id:
        last_date = tap_data[-1].split(";")[8]
        new_state = singer.write_bookmark(state, "properties",
                                          "date_sale", last_date)

        singer.write_state(new_state)

    elif bookmark_column == "date" and "stats_by_campain" in tap_stream_id:
        last_date = tap_data[-1].split(";")[0]
        last_date = last_date[0:4] + "-" + last_date[4:6] + "-" + last_date[6:8]

        new_state = singer.write_bookmark(state, "properties",
                                          "date_stats_by_campain", last_date)

        singer.write_state(new_state)

    elif bookmark_column == "date" and "stats_by_site" in tap_stream_id:
        last_date = tap_data[-1].split(";")[0]
        last_date = last_date[0:4] + "-" + last_date[4:6] + "-" + last_date[6:8]

        new_state = singer.write_bookmark(state, "properties",
                                          "date_stats_by_site", last_date)

        singer.write_state(new_state)

    elif bookmark_column == "date" and "stats_by_day" in tap_stream_id:
        last_date = tap_data[-1].split(";")[0]
        last_date = last_date[0:4] + "-" + last_date[4:6] + "-" + last_date[6:8]

        new_state = singer.write_bookmark(state, "properties", "date_stats_by_day", last_date)

        singer.write_state(new_state)

    elif bookmark_column == "date" and "stats_by_month" in tap_stream_id:
        last_date = tap_data[-1].split(";")[0]
        last_date = last_date[0:4] + "-" + last_date[4:6] + "-01"

        new_state = singer.write_bookmark(state, "properties", "date_stats_by_month", last_date)

        singer.write_state(new_state)

    return


def get_state_info(config, state, tap_stream_id):
    dim = 3
    try:
        state = state['value']['bookmarks']['properties']
        if tap_stream_id == "sale" \
                and state['date_sale'] is not None:
            debut = state['date_sale'][0:10]

        elif "stats_by_campain" in tap_stream_id \
                and state['date_stats_by_campain'] is not None:
            dim = 1
            debut = state['date_stats_by_campain'][0:10]

        elif "stats_by_site" in tap_stream_id \
                and state['date_stats_by_site'] is not None:
            dim = 2
            debut = state['date_stats_by_campain'][0:10]

        elif "stats_by_day" in tap_stream_id \
                and state['date_stats_by_day'] is not None:
            dim = 3
            debut = state['date_stats_by_day'][0:10]

        elif "stats_by_month" in tap_stream_id \
                and state['date_stats_by_month'] is not None:
            dim = 4
            debut = state['date_stats_by_month'][0:10]

        else:
            debut = config['debut']

    except KeyError:
        debut = config['debut']

    return dim, debut


def get_data_from_API(config, state, tap_stream_id):
    dim = 3

    dim, debut = get_state_info(config, state, tap_stream_id)

    if "stats_by_campain" in tap_stream_id:
        dim = 1
    elif "stats_by_site" in tap_stream_id:
        dim = 2
    elif "stats_by_day" in tap_stream_id:
        dim = 3
    elif "stats_by_month" in tap_stream_id:
        dim = 4

    today = datetime.now().isoformat(timespec='hours')[0:10]
    if tap_stream_id == "sale":
        champs_reqann = "idcampagne,nomcampagne,argann,idsite,cout,montant,monnaie,etat,date,dcookie,validation,cookie,tag,rappel",

        url = "https://stat.netaffiliation.com/reqann.php"
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "debut": debut,
                                             "fin": today,
                                             "champs": champs_reqann})
    elif "stats" in tap_stream_id:
        url = "https://stat.netaffiliation.com/lisann.php"
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "dim": dim,
                                             "debut": debut,
                                             "fin": today})


    if "OK" in response.text.splitlines()[0]:
        # skip 1st line telling how long the result is
        response = response.text.splitlines()[1:]
    else:
        print("Error on getting data from Kwanko (get_data_from_API())")
        return
    return response


def get_data_from_API_by_id(config, state, tap_stream_id, id, campain_or_site):
    dim, debut = get_state_info(config, state, tap_stream_id)
    dim = 3

    today = datetime.now().isoformat(timespec='hours')[0:10]
    url = "https://stat.netaffiliation.com/lisann.php"
    if "campain" in campain_or_site:
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "dim": dim,
                                             "camp": id,
                                             "debut": debut,
                                             "fin": today})

    elif "site" in campain_or_site:
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "dim": dim,
                                             "debut": debut,
                                             "fin": today,
                                             "site": id})

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
