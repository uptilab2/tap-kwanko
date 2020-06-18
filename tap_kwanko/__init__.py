#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests
from datetime import datetime, timedelta
import time

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
        schema = stream.schema.to_dict()
        singer.write_schema(
            stream_name=stream.stream,
            schema=schema,
            key_properties=stream.key_properties,
        )

        id_list = set()
        name_list = set()
        if "sale" == stream.tap_stream_id:
            tap_data_types_formulaires = get_sales_data_from_API(config, state, stream.tap_stream_id, "f")
            tap_data_types_ventes = get_sales_data_from_API(config, state, stream.tap_stream_id, "v")
            record_dict = {}
            for row in tap_data_types_formulaires:

                keys = list(stream.schema.properties.keys())
                value = row.split(";")

                record_dict['types'] = "f"

                for i in range(0, len(value)):
                    record_dict[keys[i]] = value[i]

                singer.write_records(stream.tap_stream_id, [record_dict])

            for row in tap_data_types_ventes:

                keys = list(stream.schema.properties.keys())
                value = row.split(";")
                record_dict = {}

                record_dict['types'] = "v"

                for i in range(0, len(value)):
                    record_dict[keys[i]] = value[i]

                singer.write_records(stream.tap_stream_id, [record_dict])

        elif stream.tap_stream_id in ["stats_by_campain", "stats_by_site"]:
            if stream.tap_stream_id == "stats_by_campain":
                select_data_by_campain_or_site = "campain"
            else:
                select_data_by_campain_or_site = "site"

            tap_data = get_stats_data_from_API(config, state, stream.tap_stream_id)

            for row in tap_data:
                value = row.split(";")
                id_value = value[0]
                name_value = value[1]

                id_list.add(id_value)
                name_list.add(name_value)
                continue

            id_list = list(id_list)
            name_list = list(name_list)

            for id, name in zip(id_list, name_list):
                tap_data = get_stats_data_from_API_by_id(config, state, stream.tap_stream_id, id,
                                                         select_data_by_campain_or_site)
                for row in tap_data:

                    keys = list(stream.schema.properties.keys())
                    value = row.split(";")

                    record_dict = {}
                    if stream.tap_stream_id == "stats_by_site":
                        record_dict['idsite'] = id
                        record_dict['nomsite'] = name
                    else:
                        record_dict['idcamp'] = id
                        record_dict['nomcamp'] = name

                    for j in range(0, len(value)):
                        if j == 0:  # init date and skip id and nom
                            last_date = value[0][0:4] + "-" + value[0][4:6] + "-" + value[0][6:8] + " 13:37:42 UTC"
                            record_dict[keys[0]] = last_date
                        else:
                            record_dict[keys[j + 2]] = value[j]

                    singer.write_records(stream.tap_stream_id, [record_dict])

        else:
            tap_data = get_stats_data_from_API(config, state, stream.tap_stream_id)
            for row in tap_data:

                keys = list(stream.schema.properties.keys())
                value = row.split(";")
                record_dict = {}
                for i in range(0, len(value)):
                    record_dict[keys[i]] = value[i]

                    if "stats_by_day" in stream.tap_stream_id:
                        last_date = value[0][0:4] + "-" + value[0][4:6] + "-" + value[0][6:8] + " 13:37:42 UTC"
                        record_dict[keys[0]] = last_date

                    elif "stats_by_month" in stream.tap_stream_id:
                        last_date = value[0][0:4] + "-" + value[0][4:6] + "-01 13:37:42 UTC"
                        record_dict[keys[0]] = last_date

                singer.write_records(stream.tap_stream_id, [record_dict])

        bookmark_state(stream.tap_stream_id, state)
    return


def bookmark_state(tap_stream_id, state):
    bookmark_name = "date_" + tap_stream_id
    last_date = datetime.now().isoformat()[0:10]

    if "stats_by_month" in tap_stream_id:
        last_date = last_date[0:4] + "-" + last_date[5:7] + "-01"

    new_state = singer.write_bookmark(state, "properties",
                                      bookmark_name, last_date)
    singer.write_state(new_state)

    return


def get_state_info(config, state, tap_stream_id):
    key = "date_" + tap_stream_id
    dict_dim = {"date_sale": 3,
                "date_stats_by_campain": 1,
                "date_stats_by_site": 2,
                "date_stats_by_day": 3,
                "date_stats_by_month": 4}

    try:
        state = state['bookmarks']['properties']
        debut = state[key][0:10]
    except KeyError:
        debut = config['debut']

    return dict_dim[key], debut


def get_sales_data_from_API(config, state, tap_stream_id, types):
    dim, debut = get_state_info(config, state, tap_stream_id)
    hier = datetime.now() - timedelta(days=1)
    fin = hier.isoformat()[0:10]

    champs_reqann = "idcampagne,nomcampagne,argann,idsite,nomsite,cout,montant,monnaie,etat,date,dcookie,validation,cookie,tag,rappel",

    url = "https://stat.netaffiliation.com/reqann.php"
    response = requests.get(url, params={"authl": config['authl'],
                                         "authv": config['authv'],
                                         "debut": debut,
                                         "fin": fin,
                                         "champs": champs_reqann,
                                         "types": types})
    while response.status_code != 200:
        time.sleep(60)
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "debut": debut,
                                             "fin": fin,
                                             "champs": champs_reqann,
                                             "types": types})

    if "OK" in response.text.splitlines()[0]:
        # skip 1st line telling how long the result is
        response = response.text.splitlines()[1:]
    else:
        print("Error on getting data from Kwanko (get_data_from_API())")
        return
    return response


def get_stats_data_from_API(config, state, tap_stream_id):
    dim, debut = get_state_info(config, state, tap_stream_id)

    if "stats_by_month" in tap_stream_id:
        debut = debut[0:8] + "01"
        mois_dernier = datetime.now().replace(day=1) - timedelta(days=1)
        fin = mois_dernier.isoformat()[0:10]
    else:
        hier = datetime.now() - timedelta(days=1)
        fin = hier.isoformat()[0:10]

    url = "https://stat.netaffiliation.com/lisann.php"
    response = requests.get(url, params={"authl": config['authl'],
                                         "authv": config['authv'],
                                         "dim": dim,
                                         "debut": debut,
                                         "fin": fin})
    while response.status_code != 200:
        time.sleep(60)
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "dim": dim,
                                             "debut": debut,
                                             "fin": fin})

    if "OK" in response.text.splitlines()[0]:
        # skip 1st line telling how long the result is
        response = response.text.splitlines()[1:]
    else:
        print("Error on getting data from Kwanko (get_data_from_API())")
        return
    return response


def get_stats_data_from_API_by_id(config, state, tap_stream_id, id, campain_or_site):
    dim, debut = get_state_info(config, state, tap_stream_id)
    dim = 3
    hier = datetime.now() - timedelta(days=1)
    fin = hier.isoformat()[0:10]

    url = "https://stat.netaffiliation.com/lisann.php"
    if "campain" in campain_or_site:
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "dim": dim,
                                             "camp": id,
                                             "debut": debut,
                                             "fin": fin})
        while response.status_code != 200:
            time.sleep(60)
            response = requests.get(url, params={"authl": config['authl'],
                                                 "authv": config['authv'],
                                                 "dim": dim,
                                                 "camp": id,
                                                 "debut": debut,
                                                 "fin": fin})

    elif "site" in campain_or_site:
        response = requests.get(url, params={"authl": config['authl'],
                                             "authv": config['authv'],
                                             "dim": dim,
                                             "debut": debut,
                                             "fin": fin,
                                             "site": id})

        while response.status_code != 200:
            time.sleep(60)
            response = requests.get(url, params={"authl": config['authl'],
                                                 "authv": config['authv'],
                                                 "dim": dim,
                                                 "debut": debut,
                                                 "fin": fin,
                                                 "site": id})

    if "OK" in response.text.splitlines()[0]:
        # skip 1st line telling how long the result is
        response = response.text.splitlines()[1:]
    else:
        print("Error on getting data from Kwanko (get_stats_data_from_API_by_id())")

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
