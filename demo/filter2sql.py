from seatable_api import Base

from seatable_api.sql_generator import filter2sql

api_token = "5f883e1e907bc4b4e8e98646f62bc51886cab7c0"
server_url = "http://127.0.0.1/"
base = Base(api_token,server_url)
base.auth()


#############Example 1#############
filter_conditions_1 = {}
sql_1 = filter2sql(base, 'Table1', filter_conditions_1)
print(sql_1) # SELECT * FROM Table1 LIMIT 0, 500

#############Example 2#############
filter_conditions_2 = {
    "filters":[
        {'column_name': '名称', 'filter_predicate': 'is', 'filter_term': "LINK"} # Name, text type
    ]
}
sql_2 = filter2sql(base, 'Table1', filter_conditions_2)
print(sql_2) # SELECT * FROM Table1 WHERE 名称 = 'LINK' LIMIT 0, 500

#############Example 3#############
filter_conditions_3 = {
    "filters":[
        {'column_name': '名称', 'filter_predicate': 'is', 'filter_term': "LINK"}, # Name, text type
        {'column_name': "Mul", 'filter_predicate': "has_none_of", 'filter_term': ["aa", "bb", "cc"]}, # Mul, multiple choice type
    ]
}
sql_3 = filter2sql(base, 'Table1', filter_conditions_3)
print(sql_3) # SELECT * FROM Table1 WHERE 名称 = 'LINK' And Mul has none of ('aa', 'bb', 'cc') LIMIT 0, 500

#############Example 4#############
filter_conditions_4 = {
    "filters": [
        {'column_name': '名称', 'filter_predicate': 'contains', 'filter_term': "LINK"},
        {'column_name': 'Creater', 'filter_predicate': 'does_not_contain', 'filter_term':['87d485c2281a42adbddb137a1070f395@auth.local']}
    ],
    "filter_conjunction": 'Or',
}
sql_4 = filter2sql(base, 'Table1', filter_conditions_4)
print(sql_4)
#SELECT * FROM Table1 WHERE 名称 like '%LINK%' Or Creater != '87d485c2281a42adbddb137a1070f395@auth.local' LIMIT 0, 500

#############Example 5#############
filter_conditions_5 = {
    "filters": [
        {'column_name': '名称', 'filter_predicate': 'contains', 'filter_term': "LINK"},
        {'column_name': "createTime", 'filter_predicate': "is_before", 'filter_term': "31", 'filter_term_modifier': "number_of_days_ago"},
        {'column_name': 'NumF', 'filter_predicate': 'greater', 'filter_term':4} # formular
    ],
    "filter_conjunction": 'Or',
    "sorts":[
        {'column_name': '名称', 'sort_type': 'up'}
    ]
}
sql_5 = filter2sql(base, 'Table1', filter_conditions_5)
print(sql_5)
#SELECT * FROM Table1 WHERE 名称 like '%LINK%' Or createTime < '2021-11-05' and createTime is not null Or NumF > 4 LIMIT 0, 500

#SELECT * FROM Table1 WHERE 名称 like '%LINK%' Or Creater != '87d485c2281a42adbddb137a1070f395@auth.local' Or NumF > 4 LIMIT 0, 500


filter_conditions_6 = {
    "filters": [
        {'column_name': '名称', 'filter_predicate': 'is', 'filter_term': "LINK"},
        {'column_name': "Mul", 'filter_predicate': "has_none_of", 'filter_term': ["aa", "bb", "cc"]}
    ],
    "filter_conjunction": 'And',
    "sorts":[
        {'column_name':'名称', 'sort_type': 'up'},
        {'column_name': 'Mul'}
    ]
}
sql_6 = filter2sql(base, 'Table1', filter_conditions_6)
print(sql_6)
# SELECT * FROM Table1 WHERE 名称 = 'LINK' And Mul has none of ('aa', 'bb', 'cc') ORDER BY 名称 ASC, Mul DESC LIMIT 0, 500
