import pymysql
from seatable import SeaTable

server_url = 'http://cloud.seatable.cn'
api_token = ''

host = 'localhost'
user = ''
password = ''
db = 'seatable'


def sync_mysql():
    """Sync database into the table
    """
    # seatable data
    seatable = SeaTable(server_url, api_token)
    table_name = 'Table1'
    rows = seatable.load_rows(table_name)
    row_keys = [row.get('Name') for row in rows]

    # mysql data
    connection = pymysql.connect(host=host, user=user, password=password, db=db)
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        sql = "SELECT * FROM order"
        cursor.execute(sql)
        mysql_data = cursor.fetchall()

    # sync
    for data in mysql_data:
        if data.get('name') not in row_keys:
            row_data = {'Name': data.get('name')}
            seatable.append_row(row_data)


if __name__ == '__main__':
    sync_mysql()
