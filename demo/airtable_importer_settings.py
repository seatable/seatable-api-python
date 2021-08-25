"""
How to get Airtable api key: https://support.airtable.com/hc/en-us/articles/360056249614-Creating-a-read-only-API-key
How to get Airtable base id: https://support.airtable.com/hc/en-us/articles/360053709993-Connecting-Salesforce-to-Airtable-via-the-REST-API
"""


# Base
server_url = 'https://cloud.seatable.io'
api_token = 'seatable api token'


# Airtable
airtable_api_key = 'airtable api key'
airtable_base_id = 'airtable base id'


# table names in Airtable
table_names = ['Design projects', 'Tasks', 'Clients']


# no links in Airtable
link = []


# links in Airtable: [('table_name', 'column_name', 'other_table_name')]
links = [
    ('Design projects', 'client', 'Clients'),
    ('Design projects', 'task', 'Tasks'),
]
