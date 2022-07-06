import json
import os
import sys

# sys.path = []
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from seatable_api import dateutils

TIME_START = '2019-06-03 20:01'
TIME_END = '2020-05-03 13:13'
TIME_STR = '2022-04-06 09:52:49'

DATE_DIFF_FUNC_TEST = [
    {
        'func_name': 'SecondsDiff',
        'func': (dateutils.datediff, 'S'),
        'assert_value': 28919520,
    },
    {
        'func_name': 'DayDiff',
        'func': (dateutils.datediff, 'D'),
        'assert_value': 335,
    },
    {
        'func_name': 'MonthDiff',
        'func': (dateutils.datediff, 'M'),
        'assert_value': 11
    },
    {
        'func_name': 'YearDiff',
        'func': (dateutils.datediff, 'Y'),
        'assert_value': 0
    },
    {
        'func_name': 'YMDiff',
        'func': (dateutils.datediff, 'YM'),
        'assert_value': 11
    },
    {
        'func_name': 'MDDiff',
        'func': (dateutils.datediff, 'MD'),
        'assert_value': 0
    },
    {
        'func_name': 'YDDiff',
        'func': (dateutils.datediff, 'YD'),
        'assert_value': 335
    },
    {
        'func_name': 'Days',
        'func': (dateutils.days, None),
        'assert_value': 334
    },
    {
        'func_name': 'Hours',
        'func': (dateutils.hours, None),
        'assert_value': 8033
    },
    {
        'func_name': 'Months',
        'func': (dateutils.months, None),
        'assert_value': 11
    },

]

DATE_ADD_FUNC_TEST = [
    {
        'func_name': 'DateAddYears',
        'func': (dateutils.dateadd, 'years'),
        'assert_value': '2021-06-03 20:01'
    },
    {
        'func_name': 'DateAddMonths',
        'func': (dateutils.dateadd, 'months'),
        'assert_value': '2019-08-03 20:01'
    },
    {
        'func_name': 'DateAddWeeks',
        'func': (dateutils.dateadd, 'weeks'),
        'assert_value': '2019-06-17 20:01'
    },
    {
        'func_name': 'DateAddDays',
        'func': (dateutils.dateadd, 'days'),
        'assert_value': '2019-06-05 20:01'
    },
    {
        'func_name': 'DateAddHours',
        'func': (dateutils.dateadd, 'hours'),
        'assert_value': '2019-06-03 22:01'
    },
    {
        'func_name': 'DateAddMinutes',
        'func': (dateutils.dateadd, 'minutes'),
        'assert_value': '2019-06-03 20:03'
    },

]

DATE_STR_FUNC_TEST = [
    {
        'func_name': 'Emonth',
        'func': (dateutils.eomonth, None),
        'assert_value': '2022-05-31'
    },
    {
        'func_name': 'Year',
        'func': (dateutils.year, None),
        'assert_value': 2022
    },
    {
        'func_name': 'Day',
        'func': (dateutils.day, None),
        'assert_value': 6
    },
    {
        'func_name': 'Minute',
        'func': (dateutils.minute, None),
        'assert_value': 52
    },
    {
        'func_name': 'Second',
        'func': (dateutils.second, None),
        'assert_value': 49
    },
    {
        'func_name': 'Month',
        'func': (dateutils.month, None),
        'assert_value': 4
    },
]


def date_diff_func_test(start, end):
    result_list = []
    for test_fun in DATE_DIFF_FUNC_TEST:
        func_name = test_fun.get('func_name')
        func, param = test_fun.get('func')
        assert_value = test_fun.get('assert_value')
        if param:
            python_value = func(start, end, param)
        else:
            python_value = func(start, end)
        if str(python_value) != str(assert_value):
            result_list.append({
                'column_name': func_name,
                'python_value': python_value,
                'assert_value': assert_value
            })

    return result_list


def date_add_func_test(time_str):
    result_list = []
    for test_fun in DATE_ADD_FUNC_TEST:
        func_name = test_fun.get('func_name')
        func, param = test_fun.get('func')
        assert_value = test_fun.get('assert_value')
        python_value = func(time_str, 2, param)
        if str(python_value) != str(assert_value):
            result_list.append({
                'column_name': func_name,
                'python_value': python_value,
                'assert_value': assert_value
            })
    return result_list


def date_func_test(time_str):
    result_list = []
    for test_fun in DATE_STR_FUNC_TEST:
        func_name = test_fun.get('func_name')
        func, param = test_fun.get('func')
        assert_value = test_fun.get('assert_value')
        python_value = func(time_str)
        if str(python_value) != str(assert_value):
            result_list.append({
                'column_name': func_name,
                'python_value': python_value,
                'assert_value': assert_value
            })
    return result_list


def dateutils_test():
    time_start = TIME_START
    time_end = TIME_END
    ctime = TIME_STR
    rel = date_diff_func_test(time_start, time_end)
    rel_1 = date_add_func_test(time_start)
    rel_2 = date_func_test(ctime)

    rel.extend(rel_1)
    rel.extend(rel_2)
    return rel


if __name__ == '__main__':


    result = dateutils_test()
    result_str = ''

    if result:
        result_str = "\n".join([json.dumps(res) for res in result])

    failed_num = len(result)
    if failed_num > 0:
        raise ValueError("Date utils test failed: %s" % result_str)