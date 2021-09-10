"""
How to get the api key from Airtable: https://support.airtable.com/hc/en-us/articles/360056249614-Creating-a-read-only-API-key
How to get the base id from Airtable: https://support.airtable.com/hc/en-us/articles/360053709993-Connecting-Salesforce-to-Airtable-via-the-REST-API
"""


# SeaTable
server_url = 'https://cloud.seatable.io'
api_token = 'seatable api token (from the target base)'


# Airtable
airtable_api_key = 'airtable api key'
airtable_base_id = 'airtable base id'


# table names in Airtable [('name_of_table_1', 'name_of_table_2', '...'])
table_names = ['Design projects', 'Tasks', 'Clients']


# links in Airtable: [('table_name', 'column_name', 'other_table_name')]
# if there are no links in all tables, you still have to provide "links = []"
links = [
    ('Design projects', 'Client', 'Clients'),
    ('Design projects', 'Tasks', 'Tasks'),
]
