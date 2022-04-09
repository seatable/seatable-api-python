from seatable_api import Base, dateutils
import json
API_TOKEN = "6e8eb3c52cf7d203632cb8225bb132a645250e73"
DTABLE_WEB_SERVER_URL = "https://dev.seatable.cn"



DATE_DIFF_FUNC_TEST = [
    {
        'col_name': 'SecondsDiff',
        'func': (dateutils.datediff, 'S')
    },
    {
        'col_name': 'DayDiff',
        'func': (dateutils.datediff, 'D')
    },
    {
        'col_name': 'MonthDiff',
        'func': (dateutils.datediff, 'M')
    },
    {
        'col_name': 'YearDiff',
        'func': (dateutils.datediff, 'Y')
    },
    {
        'col_name': 'YMDiff',
        'func': (dateutils.datediff, 'YM')
    },
    {
        'col_name': 'MDDiff',
        'func': (dateutils.datediff, 'MD')
    },
    {
        'col_name': 'YDDiff',
        'func': (dateutils.datediff, 'YD')
    },
    {
        'col_name': 'Days',
        'func': (dateutils.days, None)
    },
    {
        'col_name': 'Hours',
        'func': (dateutils.hours, None)
    },
    {
        'col_name': 'Months',
        'func': (dateutils.months, None)
    },

]

DATE_ADD_FUNC_TEST = [
    {
        'col_name': 'DateAddYears',
        'func': (dateutils.dateadd, 'years')
    },
    {
        'col_name': 'DateAddMonths',
        'func': (dateutils.dateadd, 'months')
    },
    {
        'col_name': 'DateAddWeeks',
        'func': (dateutils.dateadd, 'weeks')
    },
    {
        'col_name': 'DateAddDays',
        'func': (dateutils.dateadd, 'days')
    },
    {
        'col_name': 'DateAddHours',
        'func': (dateutils.dateadd, 'hours')
    },
    {
        'col_name': 'DateAddMinutes',
        'func': (dateutils.dateadd, 'minutes')
    },

]

DATE_STR_FUNC_TEST = [
    {
        'col_name': 'Emonth',
        'func': (dateutils.emonth, None)
    },
    {
        'col_name': 'Year',
        'func': (dateutils.year, None)
    },
    {
        'col_name': 'Day',
        'func': (dateutils.day, None)
    },
    {
        'col_name': 'Hour',
        'func': (dateutils.hour, None)
    },
    {
        'col_name': 'Minute',
        'func': (dateutils.minute, None)
    },
    {
        'col_name': 'Second',
        'func': (dateutils.second, None)
    },
    {
        'col_name': 'Month',
        'func': (dateutils.month, None)
    },
]


def date_diff_func_test(row, start, end):
    result_list = []
    for test_fun in DATE_DIFF_FUNC_TEST:
        col_name = test_fun.get('col_name')
        func, param = test_fun.get('func')
        js_value = row.get(col_name)
        if param:
            python_value = func(start, end, param)
        else:
            python_value = func(start, end)

        if str(python_value) != str(js_value):
            result_list.append({
                'column_name': col_name,
                'python_value': python_value,
                'js_value': js_value
            })

    return result_list


def date_add_func_test(row, time_str):
    result_list = []
    for test_fun in DATE_ADD_FUNC_TEST:
        col_name = test_fun.get('col_name')
        func, param = test_fun.get('func')
        js_value = row.get(col_name)
        python_value = func(time_str, 2, param)
        if str(python_value) != str(js_value):
            result_list.append({
                'column_name': col_name,
                'python_value': python_value,
                'js_value': js_value
            })
    return result_list


def date_func_test(row, time_str):
    result_list = []
    for test_fun in DATE_STR_FUNC_TEST:
        col_name = test_fun.get('col_name')
        func, param = test_fun.get('func')
        js_value = row.get(col_name)
        python_value = func(time_str)
        if str(python_value) != str(js_value):
            result_list.append({
                'column_name': col_name,
                'python_value': python_value,
                'js_value': js_value
            })
    return result_list


def dateutils_test(base):
    rel, rel_1, rel_2 = [], [], []
    for row in base.list_rows('Table1'):
        time_start = row.get('DT1')
        time_end = row.get('DT2')
        ctime = row.get('CTime')
        rel = date_diff_func_test(row, time_start, time_end)
        rel_1 = date_add_func_test(row, time_start)
        rel_2 = date_func_test(row, ctime)

    rel.extend(rel_1)
    rel.extend(rel_2)
    return rel


if __name__ == '__main__':
    base = Base(API_TOKEN, DTABLE_WEB_SERVER_URL)
    base.auth()

    result = dateutils_test(base)
    result_str = ''

    if result:
        result_str = "\n".join([json.dumps(res) for res in result])

    base.append_row('TestResult', {
        "FailedNum": len(result),
        "Details": result_str
    })
