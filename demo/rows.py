from seatable_api import SeaTableAPI, Base
from seatable_api.constants import ColumnTypes
server_url = 'https://dev.seatable.cn'
api_token = 'f8c03507d022437ba9874dd54f7046a112a9b1fb'


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


def queryset_filter():
    base = Base(api_token, server_url)
    base.auth()

    table_name = 'Table1'

    queryset = base.filter(table_name, "age>18 and gender='male'")

    copy_queryset = queryset.all()

    tony_row = queryset.get("'first name'='tony'")

    first_row = queryset.first()

    last_row = queryset.last()

    count = queryset.count()

    exists = queryset.exists()

    elder_queryset = queryset.filter("age > 70")
    for row in elder_queryset:
        print(row)

    update_row_data = {'paid': True}
    updated_rows = elder_queryset.update(update_row_data)

    deleted_count = elder_queryset.delete()


if __name__ == '__main__':
    # filter_rows()
    # row_link()
    # queryset_filter()
    base = Base(api_token, server_url)
    base.auth()

    print(base.list_columns('Table1'))
    # base.rename_column('Table1', 'PR', 'PR1')
    # base.resize_column('Table1', 'PR1', 200)
    # base.freeze_column('Table1', 'PR1', frozen=False)
    # base.move_column('Table1', '描述', 'PR1')
    # base.modify_column_type('Table1', 'New Test', ColumnTypes.NUMBER)
    base.delete_column('Table1', 'New Test')