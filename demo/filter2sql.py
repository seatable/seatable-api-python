from seatable_api import Base

from seatable_api import filter2sql

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
        {'column_name': 'Name', 'filter_predicate': 'is', 'filter_term': "LINK"} # Name, text type
    ]
}
sql_2 = filter2sql(base, 'Table1', filter_conditions_2)
print(sql_2) # SELECT * FROM Table1 WHERE Name = 'LINK' LIMIT 0, 500
#############Example 3#############
filter_conditions_3 = {
    "filters":[
        {'column_name': 'Name', 'filter_predicate': 'is', 'filter_term': "LINK"}, # Name, text type
        {'column_name': "Mul", 'filter_predicate': "has_none_of", 'filter_term': ["aa", "bb", "cc"]}, # Mul, multiple choice type
    ]
}
sql_3 = filter2sql(base, 'Table1', filter_conditions_3)
print(sql_3) # SELECT * FROM Table1 WHERE Name = 'LINK' And Mul has none of ('aa', 'bb', 'cc') LIMIT 0, 500

#############Example 4#############
filter_conditions_4 = {
    "filters": [
        {'column_name': 'Name', 'filter_predicate': 'contains', 'filter_term': "LINK"},
        {'column_name': 'Creater', 'filter_predicate': 'does_not_contain', 'filter_term':['87d485c2281a42adbddb137a1070f395@auth.local']}
    ],
    "filter_conjunction": 'Or',
}
sql_4 = filter2sql(base, 'Table1', filter_conditions_4)
print(sql_4)
#SELECT * FROM Table1 WHERE Name like '%LINK%' Or Creater != '87d485c2281a42adbddb137a1070f395@auth.local' LIMIT 0, 500

#############Example 5#############
filter_conditions_5 = {
    "filters": [
        {'column_name': 'Name', 'filter_predicate': 'contains', 'filter_term': "LINK"},
        {'column_name': "createTime", 'filter_predicate': "is_before", 'filter_term': "31", 'filter_term_modifier': "number_of_days_ago"},
        {'column_name': 'NumF', 'filter_predicate': 'greater', 'filter_term':4} # formular
    ],
    "filter_conjunction": 'Or',
    "sorts":[
        {'column_name': 'Name', 'sort_type': 'up'}
    ]
}
sql_5 = filter2sql(base, 'Table1', filter_conditions_5)
print(sql_5)
#SELECT * FROM Table1 WHERE Name like '%LINK%' Or createTime < '2021-11-05' and createTime is not null Or NumF > 4 LIMIT 0, 500

#############Example 6#############
filter_conditions_6 = {
    "filters": [
        {'column_name': 'Name', 'filter_predicate': 'is', 'filter_term': "LINK"},
        {'column_name': "Mul", 'filter_predicate': "has_none_of", 'filter_term': ["aa", "bb", "cc"]}
    ],
    "filter_conjunction": 'And',
    "sorts":[
        {'column_name':'Name', 'sort_type': 'up'},
        {'column_name': 'Mul'}
    ]
}
sql_6 = filter2sql(base, 'Table1', filter_conditions_6)
print(sql_6)
# SELECT * FROM Table1 WHERE Name = 'LINK' And Mul has none of ('aa', 'bb', 'cc') ORDER BY Name ASC, Mul DESC LIMIT 0, 500

#############Example 7#############
filter_conditions_7 = {
    "filters": [
        {'column_name': 'LinkF', 'filter_predicate': 'has_all_of', 'filter_term': ["AA", "AAA"]},
        {'column_name': "Mul", 'filter_predicate': "has_none_of", 'filter_term': ["aa", "bb", "cc"]}
    ],
    "filter_conjunction": 'And',
    "sorts":[
        {'column_name':'Name', 'sort_type': 'up'},
    ]
}
sql_7 = filter2sql(base, 'Table1', filter_conditions_7)
print(sql_7)
# SELECT * FROM Table1 WHERE LinkF has all of ('AA', 'AAA') And Mul has none of ('aa', 'bb', 'cc') ORDER BY Name ASC LIMIT 0, 500

#############Example 8#############
filter_conditions_8 = {
    "filter_groups": [
        {
            "filters": [
                {'column_name': 'Name', 'filter_predicate': 'is', 'filter_term': "LINK"},
                {'column_name': "createTime", 'filter_predicate': "is_before", 'filter_term': "31", 'filter_term_modifier': "number_of_days_ago"},
        ],
            "filter_conjunction": 'And'
        },
        {
            "filters": [
                {'column_name': "Mul", 'filter_predicate': "has_none_of", 'filter_term': ["aa", "bb", "cc"]},
                {'column_name': "CB", 'filter_predicate': "is", 'filter_term': False},
        ],
            "filter_conjunction": 'Or'}
    ],
    "group_conjunction": 'And',
    "sorts":[
        {'column_name':'Name'}
    ]
}
sql_8 = filter2sql(base, 'Table1', filter_conditions_8, by_group=True)
print(sql_8)

# SELECT * FROM Table1 WHERE (Name = 'LINK' And createTime < '2021-11-06' and createTime is not null) And (Mul has none of ('aa', 'bb', 'cc') Or CB = False) ORDER BY Name DESC LIMIT 0, 500
