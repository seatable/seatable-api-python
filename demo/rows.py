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
            "filter_term_modifier": ""
        },
        {
            "column_name": "Name",
            "filter_predicate": "contains",
            "filter_term": "b",
            "filter_term_modifier": ""
        }
    ]

    filtered_rows = seatable.filter_rows(table_name, filters=filters, filter_conjunction='Or')


if __name__ == '__main__':
    filter_rows()
