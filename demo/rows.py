from seatable_api import SeaTableAPI

server_url = 'http://127.0.0.1:8000'
api_token = 'd67d4e0eeee24b55ff7b60595faed7e2df36e1d1'


def filter_rows():
    seatable = SeaTableAPI(api_token, server_url)
    seatable.auth()

    table_name = 'Table1'

    filters = [
        {
            "column_name": "Name",
            "filter_predicate": "contains",
            "filter_term": "a",
        },
        {
            "column_name": "Name",
            "filter_predicate": "contains",
            "filter_term": "b",
        }
    ]

    filtered_rows = seatable.filter_rows(table_name, filters=filters, filter_conjunction='Or')


def row_link():

    table_name = 'Table1'
    other_table_name = 'Table2'

    seatable = SeaTableAPI(api_token, server_url)
    seatable.auth()

    rows = seatable.list_rows(table_name)
    other_rows = seatable.list_rows(other_table_name)

    row = rows[0]
    row_id = row['_id']

    other_row = other_rows[1]
    other_row_id = other_row['_id']

    seatable.add_link(table_name, other_table_name, row_id, other_row_id)

    # seatable.remove_link(table_name, other_table_name, row_id, other_row_id)


if __name__ == '__main__':
    filter_rows()
    row_link()
