import pymysql
from seatableapi import SeaTableAPI

server_url = 'http://cloud.seatable.cn'
api_token = ''


def sync_mysql():
    """Sync database into the table
    """
    # seatable data
    seatable_api = SeaTableAPI(server_url, api_token)
    seatable_api.auth()

    table_name = 'Table1'
    rows = seatable_api.load_rows(table_name)
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
            seatable_api.append_row(table_name, row_data)


if __name__ == '__main__':
    sync_mysql()
