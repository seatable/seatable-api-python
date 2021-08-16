"""
1. First create a blank Base in SeaTable webpage.
2. Generate API Token in the Base.
3. Get Airtable api key and Airtable base id in Airtable webpage.
4. Base.auth


How to get Airtable api key: https://support.airtable.com/hc/en-us/articles/360056249614-Creating-a-read-only-API-key
How to get Airtable base id: https://support.airtable.com/hc/en-us/articles/360053709993-Connecting-Salesforce-to-Airtable-via-the-REST-API
Airtable field types in API: https://airtable.com/api/meta, https://support.airtable.com/hc/en-us/articles/360055885353-Field-types-reference

LINK
SINGLE_SELECT
BUTTON
"""

from seatable_api import Base, AirtableConvertor


server_url = 'http://127.0.0.1'
api_token = '0fb418b964cc08908407203f43f29e4c075e965e'

# base
base = Base(api_token, server_url)
base.auth()

# Airtable convertor
airtable_api_key = 'airtable api key'
airtable_base_id = 'airtable base id'
table_names = ['Design projects', 'Tasks', 'Clients']

convertor = AirtableConvertor(
    airtable_api_key,
    airtable_base_id,
    base,
    table_names
)

convertor.convert_tables()

convertor.convert_rows()
