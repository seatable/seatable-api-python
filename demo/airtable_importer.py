#!/usr/bin/env python3

import sys
from seatable_api import Base, AirtableConvertor
from airtable_importer_settings import server_url, api_token, airtable_api_key, airtable_base_id, \
    table_names, first_columns, links


def get_convertor():
    base = Base(api_token, server_url)
    base.auth()
    convertor = AirtableConvertor(
        airtable_api_key=airtable_api_key,
        airtable_base_id=airtable_base_id,
        base=base,
        table_names=table_names,
        first_columns=first_columns,
        links=links,
    )
    return convertor


def import_header():
    convertor = get_convertor()
    convertor.convert_metadata()


def import_rows():
    convertor = get_convertor()
    convertor.convert_data()


if __name__ == '__main__':
    argv_info = '\nusage :\npython3 airtable_importer.py { --import-header | --import-rows }\n'

    if len(sys.argv) != 2:
        print(argv_info)
    else:
        if sys.argv[1] == '--import-header':
            import_header()
        elif sys.argv[1] == '--import-rows':
            import_rows()
        else:
            print(argv_info)
