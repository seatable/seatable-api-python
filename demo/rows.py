from seatable_api import SeaTableAPI, Base

server_url = 'http://127.0.0.1:8000'
api_token = 'd67d4e0eeee24b55ff7b60595faed7e2df36e1d1'


def query():
    base = Base(api_token, server_url)
    base.auth()

    sql = 'select name,price from table where year = 2021;'
    results = base.query(sql)


def row_link():

    table_name = 'Table1'
    other_table_name = 'Table2'
    column_name = 'Foreign Key'

    seatable = SeaTableAPI(api_token, server_url)
    seatable.auth()

    # get column link id
    metadata = seatable.get_metadata()

    table = None
    for table_item in metadata['tables']:
        if table_item['name'] == table_name:
            table = table_item
            break

    column = None
    for column_item in table['columns']:
        if column_item['name'] == column_name:
            column = column_item
            break

    link_id = column['data']['link_id']

    # get row id
    rows = seatable.list_rows(table_name)
    other_rows = seatable.list_rows(other_table_name)
    row_id = rows[0]['_id']
    other_row_id = other_rows[0]['_id']

    # add link
    seatable.add_link(link_id, table_name, other_table_name,row_id, other_row_id)

    # remove link
    # seatable.remove_link(link_id, table_name, other_table_name, row_id, other_row_id)


if __name__ == '__main__':
    row_link()
    query()
