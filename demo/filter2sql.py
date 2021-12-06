from seatable_api import Base

from seatable_api.sql_generator import filter2sql

api_token = "5f883e1e907bc4b4e8e98646f62bc51886cab7c0"
server_url = "http://127.0.0.1/"
filter_conditions = {
    # "filters": [
    #     {'column_name': '名称', 'filter_predicate': 'is', 'filter_term': "LINK"},
    #     # {'column_name': "createTime", 'filter_predicate': "is_before", 'filter_term': "31", 'filter_term_modifier': "number_of_days_ago"},
    #     # {'column_name': "Mul", 'filter_predicate': "has_none_of", 'filter_term': ["aa", "bb", "cc"]},
    #     # {'column_name': "CB", 'filter_predicate': "is", 'filter_term': False},
    #     # {'column_name': "Creater", 'filter_predicate': "is", 'filter_term': '87d485c2281a42adbddb137a1070f395@auth.local'},
    #     # {'column_name': "Creater", 'filter_predicate': "does_not_contain", 'filter_term': ['87d485c2281a42adbddb137a1070f395@auth.local','anonymous']}
    #     # {'column_name': "Creater", 'filter_predicate': "contains", 'filter_term': ['87d485c2281a42adbddb137a1070f395@auth.local','anonymous']},
    #     # {'column_name': "Colla", 'filter_predicate': "is_not_empty", 'filter_term': ['87d485c2281a42adbddb137a1070f395@auth.local']},
    #     # {'column_name': "rate", 'filter_predicate': "greater", 'filter_term': 2}
    # ],
    "filter_conjunction": 'And',
    # "sorts":[
    #     {'column_name':'名称',},
    #     {'column_name': 'Mul'}
    # ]
}

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