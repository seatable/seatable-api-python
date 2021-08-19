"""
How to get Airtable api key: https://support.airtable.com/hc/en-us/articles/360056249614-Creating-a-read-only-API-key
How to get Airtable base id: https://support.airtable.com/hc/en-us/articles/360053709993-Connecting-Salesforce-to-Airtable-via-the-REST-API
"""

from seatable_api import Base, AirtableConvertor


server_url = 'http://127.0.0.1'
api_token = '0fb418b964cc08908407203f43f29e4c075e965e'

# base
base = Base(api_token, server_url)
base.auth()

# Airtable
airtable_api_key = 'airtable api key'
airtable_base_id = 'airtable base id'

# table names in Airtable
table_names = ['Design projects', 'Tasks', 'Clients']

# links in Airtable: [('table_name', 'column_name', 'other_table_name')]
links = [
    ('Design projects', 'clients', 'Clients'),
    ('Design projects', 'tasks', 'Tasks'),
]

convertor = AirtableConvertor(
    airtable_api_key,
    airtable_base_id,
    base,
    table_names,
    links,
)

convertor.auto_convert()
