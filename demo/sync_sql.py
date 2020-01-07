import pymysql
from seatable_api import SeaTableAPI

server_url = 'http://127.0.0.1:8000'
api_token = '48b6a41a0c2e1ee4bf294ed42445914025a0a60c'


def sync_mysql():
    """Sync database into the table
    """
    # seatable data
    seatable = SeaTableAPI(api_token, server_url)
    seatable.auth()

    table_name = 'Table1'
    rows = seatable.list_rows(table_name)
    row_keys = [row.get('Name') for row in rows]

    # mysql data
    host = 'localhost'
    user = ''
    password = ''
    db = 'seatable'
    connection = pymysql.connect(host=host, user=user, password=password, db=db)

    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        sql = "SELECT * FROM order"
        cursor.execute(sql)
        mysql_data = cursor.fetchall()

    # sync
    for item in mysql_data:
        if item.get('name') not in row_keys:
            row_data = {
                'Name': item.get('name'),
            }
            seatable.append_row(table_name, row_data)


if __name__ == '__main__':
    sync_mysql()
