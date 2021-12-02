from seatable_api.constants import TEXT_CONDITIOINS_MAPPING, NUMBER_COMDITIONS_MAPPING, ColumnTypes, \
    FilterTermModifier, FilterPredicateTypes
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def text_condition(column, filter_item):
    column_name = column.get('name')
    filter_predicate = filter_item.get('filter_predicate')
    filter_term = filter_item.get('filter_term')
    if filter_predicate in [FilterPredicateTypes.CONTAINS, FilterPredicateTypes.NOT_CONTAIN]:
        return "%s %s '%%%s%%'" % (
                column_name,
                TEXT_CONDITIOINS_MAPPING.get(filter_predicate, '='),
                filter_term
    )
    return "%s %s '%s'" % (
        column_name,
        TEXT_CONDITIOINS_MAPPING.get(filter_predicate, '='),
        filter_term
    )

def number_condition(column, filter_item):
    column_name = column.get('name')
    filter_predicate = filter_item.get('filter_predicate')
    filter_term = filter_item.get('filter_term')
    return "%s %s %s" % (
        column_name,
        NUMBER_COMDITIONS_MAPPING.get(filter_predicate, '='),
        filter_term
    )

def single_select_condition(column, filter_item):
    column_name = column.get('name')
    filter_predicate = filter_item.get('filter_predicate')
    filter_term = filter_item.get('filter_term')
    options = column.get('data', {}).get('options', {})
    if filter_predicate == FilterPredicateTypes.IS:
        option_name = filter_term
        # for option in options:
        #     if option.get('id') == filter_term:
        #         option_name = option.get('name')
        #         break
        return "%(column_name)s = '%(option_name)s'" % ({
            "column_name": column_name,
            "option_name": option_name
        })

    if filter_predicate == FilterPredicateTypes.IS_NOT:
        option_name = filter_term
        # for option in options:
        #     if option.get('id') == filter_term:
        #         option_name = option.get('name')
        #         break
        return "%(column_name)s <> '%(option_name)s'" % ({
            "column_name": column_name,
            "option_name": option_name
        })

    if filter_predicate == FilterPredicateTypes.IS_ANY_OF:
        if not isinstance(filter_term, list):
            filter_term = [filter_term, ]
        option_names = ["'%s'" % (op_name) for op_name in filter_term]
        # option_id_name_map = {option.get('id'): option.get('name') for option in options}
        # for option_id in filter_term:
        #     if option_id in option_id_name_map.keys():
        #         option_names.append("'%s'" % option_id_name_map.get(option_id))
        return "%(column_name)s in (%(option_names)s)" % ({
            "column_name": column_name,
            "option_names": ", ".join(option_names)
        })

    if filter_predicate == FilterPredicateTypes.IS_NONE_OF:
        if not isinstance(filter_term, list):
            filter_term = [filter_term, ]
        option_names = ["'%s'" % (op_name) for op_name in filter_term]
        # option_id_name_map = {option.get('id'): option.get('name') for option in options}
        # for option_id in filter_term:
        #     if option_id in option_id_name_map.keys():
        #         option_names.append("'%s'" % option_id_name_map.get(option_id))
        return "%(column_name)s not in (%(option_names)s)" % ({
            "column_name": column_name,
            "option_names": ", ".join(option_names)
        })

    if filter_predicate == FilterPredicateTypes.EMPTY:
        return "%(column_name)s is null" % ({'column_name': column_name})

    if filter_predicate == FilterPredicateTypes.NOT_EMPTY:
        return "%(column_name)s is not null" % ({'column_name': column_name})

def multiple_select_condition(column, filter_item):
    column_name = column.get('name')
    print(column_name, 'aaaaaa')
    filter_predicate = filter_item.get('filter_predicate')
    filter_term = filter_item.get('filter_term')
    # options = column.get('data', {}).get('options', [])
    # option_names = []
    # for option in options:
    #     if option.get('id') in filter_term:
    #         option_names.append("'%s'" % option.get('name'))
    if not filter_term:
        if filter_predicate == FilterPredicateTypes.NOT_EMPTY:
            return "%(column_name)s is not null" % ({'column_name': column_name})

        if filter_predicate == FilterPredicateTypes.EMPTY:
            return "%(column_name)s is null" % ({'column_name': column_name})

        return ''
    option_names = ["'%s'" % op_name for op_name in filter_term]
    option_names_str = ', '.join(option_names)
    if filter_predicate == FilterPredicateTypes.HAS_ANY_OF:
        return "%(column_name)s in (%(option_names_str)s)" % ({
            "column_name": column_name,
            "option_names_str": option_names_str
        })

    if filter_predicate == FilterPredicateTypes.HAS_ALL_OF:
        return "%(column_name)s has all of (%(option_names_str)s)" % ({
            "column_name": column_name,
            "option_names_str": option_names_str
        })

    if filter_predicate == FilterPredicateTypes.HAS_NONE_OF:
        return "%(column_name)s has none of (%(option_names_str)s)" % ({
            "column_name": column_name,
            "option_names_str": option_names_str
        })

    if filter_predicate == FilterPredicateTypes.IS_EXACTLY:
        return "%(column_name)s is exactly (%(option_names_str)s)" % ({
            "column_name": column_name,
            "option_names_str": option_names_str
        })

def _other_date(filter_term_modifier, filter_term):
    today = datetime.today()
    year = today.year
    month = today.month
    day = today.day

    def _get_end_day_of_month(year, month):

        days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            days[1] = 29

        return days[month-1]

    if filter_term_modifier == FilterTermModifier.TODAY:
        return today

    if filter_term_modifier == FilterTermModifier.TOMORROW:
        tomorrow = today + timedelta(days=1)
        return tomorrow

    if filter_term_modifier == FilterTermModifier.YESTERDAY:
        yesterday = today - timedelta(days=1)
        return yesterday

    if filter_term_modifier == FilterTermModifier.ONE_WEEK_AGO:
        one_week_ago = today - timedelta(days=7)
        return one_week_ago

    if filter_term_modifier == FilterTermModifier.ONE_WEEK_FROM_NOW:
        one_week_from_now = today + timedelta(days=7)
        return one_week_from_now

    if filter_term_modifier == FilterTermModifier.ONE_MONTH_AGO:
        one_month_ago = today - relativedelta(months=1)
        return one_month_ago

    if filter_term_modifier == FilterTermModifier.ONE_MONTH_FROM_NOW:
        one_month_from_now = today + relativedelta(months=1)
        return one_month_from_now

    if filter_term_modifier == FilterTermModifier.NUMBER_OF_DAYS_AGO:
        days_ago = today - timedelta(days=int(filter_term))
        return days_ago

    if filter_term_modifier == FilterTermModifier.NUMBER_OF_DAYS_FROM_NOW:
        days_after = today + timedelta(days=int(filter_term))
        return days_after

    if filter_term_modifier == FilterTermModifier.EXACT_DATE:
        return datetime.strptime(filter_term, "%Y-%m-%d").date()

    if filter_term_modifier == FilterTermModifier.THE_PAST_WEEK:
        week_day = today.isoweekday() + 1 # 1-7
        start_date = today - timedelta(days =(week_day + 6))
        end_date = today - timedelta(days=week_day)
        return start_date, end_date

    if filter_term_modifier == FilterTermModifier.THIS_WEEK:
        week_day = today.isoweekday() + 1
        start_date = today - timedelta(days=week_day-1)
        end_date = today + timedelta(days=7-week_day)
        return start_date, end_date

    if filter_term_modifier == FilterTermModifier.THE_NEXT_WEEK:
        week_day = today.isoweekday() + 1
        start_date = today + timedelta(days=7-week_day)
        end_date = today + timedelta(days=14-week_day)
        return start_date, end_date

    if filter_term_modifier == FilterTermModifier.THE_PAST_MONTH:
        one_month_ago = today - relativedelta(months=1)
        one_month_ago_year = one_month_ago.year
        one_month_ago_month = one_month_ago.month
        one_month_age_end_day = _get_end_day_of_month(one_month_ago_year, one_month_ago_month)
        start_date = datetime(one_month_ago_year, one_month_ago_month, 1)
        end_date = datetime(one_month_ago_year, one_month_ago_month, one_month_age_end_day)
        return start_date, end_date

    if filter_term_modifier == FilterTermModifier.THIS_MONTH:
        current_month = today.month
        current_year = today.year
        current_month_end_day = _get_end_day_of_month(current_year, current_month)
        start_date = datetime(current_year,current_month, 1)
        end_date = datetime(current_year, current_month, current_month_end_day)
        return start_date, end_date

    if filter_term_modifier == FilterTermModifier.THE_NEXT_MONTH:
        next_month = today + relativedelta(months=1)
        next_month_year = next_month.year
        next_month_month = next_month.month
        next_month_end_day = _get_end_day_of_month(next_month_year, next_month_month)
        start_date = datetime(next_month_year, next_month_month, 1)
        end_date = datetime(next_month_year, next_month_month, next_month_end_day)
        return start_date, end_date

    if filter_term_modifier == FilterTermModifier.THE_PAST_YEAR:
        last_year = year - 1
        start_date = datetime(last_year, 1, 1)
        end_date = datetime(last_year, 12, 31)
        return start_date, end_date

    if filter_term_modifier == FilterTermModifier.THIS_YEAR:
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        return start_date, end_date

    if filter_term_modifier == FilterTermModifier.THE_NEXT_YEAR:
        next_year = year + 1
        start_date = datetime(next_year, 1, 1)
        end_date = datetime(next_year, 12, 31)
        return start_date, end_date

    if filter_term_modifier == FilterTermModifier.THE_NEXT_NUMBERS_OF_DAYS:
        end_date = today + timedelta(days=int(filter_term))
        return today, end_date

    if filter_term_modifier == FilterTermModifier.THE_PAST_NUMBERS_OF_DAYS:
        start_date = today - timedelta(days=int(filter_term))
        return start_date, today

def date_conditions(column, filter_item):

    def _format_date(dt):
        return dt.strftime("%Y-%m-%d")

    column_name = column.get('name')
    filter_predicate = filter_item.get('filter_predicate')
    filter_term = filter_item.get('filter_term')
    filter_term_modifier = filter_item.get('filter_term_modifier')

    filter_labels = [
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY
    ]

    if (not filter_item) and (filter_predicate not in filter_labels) and (filter_predicate == FilterTermModifier.EXACT_DATE):
        return ''

    if filter_predicate == FilterPredicateTypes.IS:
        date = _other_date(filter_term_modifier, filter_term)
        next_date = _format_date(date + timedelta(days=1))
        target_date = _format_date(date)
        return "%(column_name)s >= '%(target_date)s' and %(column_name)s < '%(next_date)s'" % ({
            "column_name": column_name,
            "target_date": target_date,
            "next_date": next_date
        })

    if filter_predicate == FilterPredicateTypes.IS_WITHIN:
        start_date, end_date = _other_date(filter_term_modifier, filter_term)
        return "%(column_name)s >= '%(start_date)s' and %(column_name)s < '%(end_date)s'" % ({
            "column_name": column_name,
            "start_date": _format_date(start_date),
            "end_date": _format_date(end_date)
        })

    if filter_predicate == FilterPredicateTypes.IS_BEFORE:
        target_date = _other_date(filter_term_modifier, filter_term)
        return "%(column_name)s < '%(target_date)s' and %(column_name)s is not null" %({
            "column_name": column_name,
            "target_date": _format_date(target_date)
        })

    if filter_predicate == FilterPredicateTypes.IS_AFTER:
        target_date = _other_date(filter_term_modifier, filter_term)
        return "%(column_name)s > '%(target_date)s'" % ({
            "column_name": column_name,
            "target_date": _format_date(target_date)
        })

    if filter_predicate == FilterPredicateTypes.IS_ON_OR_BEFORE:
        target_date = _other_date(filter_term_modifier, filter_term)
        return "%(column_name)s <= '%(target_date)s' and %(column_name)s is not null" % ({
            "column_name": column_name,
            "target_date": _format_date(target_date)
        })

    if filter_predicate == FilterPredicateTypes.IS_ON_OR_AFTER:
        target_date = _other_date(filter_term_modifier, filter_term)
        return "%(column_name)s >= '%(target_date)s' and %(column_name)s is not null" % ({
            "column_name": column_name,
            "target_date": _format_date(target_date)
        })

    if filter_predicate == FilterPredicateTypes.IS_NOT:
        target_date = _other_date(filter_term_modifier, filter_term)
        start_date = target_date - timedelta(days=1)
        end_date = target_date + timedelta(days=1)
        return "(%(column_name)s >= '%(end_date)s' or %(column_name)s <= '%(start_date)s') and %(column_name)s is not null" % ({
            "column_name":column_name,
            "start_date": _format_date(start_date),
            "end_date": _format_date(end_date)
        })

    if filter_predicate == FilterPredicateTypes.EMPTY:
        return "%(column_name)s is null" % ({'column_name': column_name})

    if filter_predicate == FilterPredicateTypes.NOT_EMPTY:
        return "%(column_name)s is not null" % ({'column_name': column_name})






def creator_condition(column, filter_item):
    column_name = column.get('name')
    filter_predicate = filter_item.get('filter_predicate')
    filter_term = filter_item.get('filter_term')

    select_collaborators = filter_term

    if filter_predicate == FilterPredicateTypes.CONTAINS:
        filter_term_str = ",".join(select_collaborators)
        return "%(column_name)s in (%(filter_term_str)s)" % ({
            "column_name": column_name,
            "filter_term_str": filter_term_str
        })

    if filter_predicate == FilterPredicateTypes.NOT_CONTAIN:
        sql_slice = []
        for colla in select_collaborators:
            sql_slice.append("%(column_name)s != %(colla)s" % ({
                "column_name": column_name,
                "colla": colla
            }))
        return ' and '.join(sql_slice)

    if filter_predicate == FilterPredicateTypes.INCLUDE_ME:
        pass

    if filter_predicate == FilterPredicateTypes.IS:
        return "%(column_name)s = %(filter_term)s" % ({
            "column_name": column_name,
            "filter_term": filter_term
        })

    if filter_predicate == FilterPredicateTypes.IS_NOT:
        return "%(column_name)s != %(filter_term)s" % ({
            "column_name": column_name,
            "filter_term": filter_term
        })



def colaborator_condition(column, filter_item):
    column_name = column.get('name')
    filter_predicate = filter_item.get('filter_predicate')
    filter_term = filter_item.get('filter_term')

    select_collaborators = filter_term

    if not select_collaborators:
        if filter_predicate == FilterPredicateTypes.INCLUDE_ME:
            pass

        if filter_predicate == FilterPredicateTypes.NOT_EMPTY:
            return "%(column_name)s is not null" % ({'column_name': column_name})

        if filter_predicate == FilterPredicateTypes.EMPTY:
            return "%(column_name)s is null" % ({'column_name': column_name})

        return ''

    colla_names_str = ', '.join(select_collaborators)
    if filter_predicate == FilterPredicateTypes.HAS_ANY_OF:
        return "%(column_name)s in (%(option_names_str)s)" % ({
            "column_name": column_name,
            "option_names_str": colla_names_str
        })

    if filter_predicate == FilterPredicateTypes.HAS_ALL_OF:
        return "%(column_name)s has all of (%(option_names_str)s)" % ({
            "column_name": column_name,
            "option_names_str": colla_names_str
        })

    if filter_predicate == FilterPredicateTypes.HAS_NONE_OF:
        return "%(column_name)s has none of (%(option_names_str)s)" % ({
            "column_name": column_name,
            "option_names_str": colla_names_str
        })

    if filter_predicate == FilterPredicateTypes.IS_EXACTLY:
        return "%(column_name)s is exactly (%(option_names_str)s)" % ({
            "column_name": column_name,
            "option_names_str": colla_names_str
        })


def checkbox_condition(column, filter_item):
    column_name = column.get('name')
    filter_term = filter_item.get('filter_term')
    return "%(column_name)s = %(value)s" % ({
        "column_name": column_name,
        "value": filter_term
    })

def get_sql_condition_by_filter(column, filter_item):
    column_type = column.get('type')
    if column_type in [
        ColumnTypes.TEXT.value,
        ColumnTypes.AUTO_NUMBER.value,
        ColumnTypes.EMAIL.value,
        ColumnTypes.GEOLOCATION.value,
        ColumnTypes.URL.value,
    ]:
        return text_condition(column, filter_item)

    if column_type in [
        ColumnTypes.DURATION.value,
        ColumnTypes.NUMBER.value,
        ColumnTypes.RATE.value,
    ]:
        return number_condition(column, filter_item)

    if column_type == ColumnTypes.CHECKBOX.value:
        return checkbox_condition(column, filter_item)

    if column_type in [
        ColumnTypes.DATE.value,
        ColumnTypes.CTIME.value,
        ColumnTypes.MTIME.value,
    ]:
        return date_conditions(column, filter_item)

    if column_type == ColumnTypes.SINGLE_SELECT.value:
        return single_select_condition(column, filter_item)

    if column_type == ColumnTypes.MULTIPLE_SELECT.value:
        return multiple_select_condition(column, filter_item)

    if column_type in [
        ColumnTypes.LAST_MODIFIER.value,
        ColumnTypes.CREATOR.value,
    ]:
        return creator_condition(column, filter_item)

    if type == ColumnTypes.COLLABORATOR.value:
        return colaborator_condition(column, filter_item)

    return text_condition(column, filter_item)



class BaseSQLGenerator(object):

    def __init__(self, authed_base, table_name, filter_conditions):
        self.base = authed_base
        self.table_name = table_name
        self.filter_conditions = filter_conditions
        self.columns = []

        self._init_columns()

    def _init_columns(self):
        if not self.columns:
            self.columns = self.base.list_columns(self.table_name)

    def _get_column_by_key(self, col_key):
        for col in self.columns:
            if col.get('key') == col_key:
                return col
        return None

    def _get_column_by_name(self, col_name):
        for col in self.columns:
            if col.get('name') == col_name:
                return col
        return None

    def _sort2sql(self):
        filter_conditions = self.filter_conditions
        condition_sorts = filter_conditions.get('sorts', [])
        if not condition_sorts:
            return ''

        order_header = 'ORDER BY '
        clauses = []
        for sort in condition_sorts:
            column_key = sort.get('column_key', '')
            sort_type = sort.get('sort_type', 'DESC') == 'up' and 'ASC' or 'DESC'
            column = self._get_column_by_key(column_key)
            if not column:
                if column_key in ['_ctime', '_mtime']:
                    order_condition = '%s %s' % (column_key, sort_type)
                    clauses.append(order_condition)
                    continue
                else:
                    continue
            order_condition = '%s %s' % (column.get('name'), sort_type)
            clauses.append(order_condition)
        if not clauses:
            return ''

        return "%s%s" % (
            order_header,
            ', '.join(clauses)
        )

    def _filter2sql(self):
        filter_conditions = self.filter_conditions
        filter_groups = filter_conditions.get('filter_groups', [])
        group_conjunction = filter_conditions.get('group_conjunction', 'And')
        if not filter_groups:
            return ''

        filter_header = 'WHERE '
        group_string_list = []
        group_conjunction_split = ' %s ' % group_conjunction
        for filter_group in filter_groups:
            filters = filter_group.get('filters')
            filter_conjunction = filter_group.get('filter_conjunction', 'And')
            filter_conjunction_split = " %s " % filter_conjunction
            filter_string_list = []
            for filter_item in filters:
                column_key = filter_item.get('column_key')
                column_name = filter_item.get('column_name')
                column = column_key and self._get_column_by_key(column_key)
                if not column:
                    column = column_name and self._get_column_by_name(column_name)
                    if not column:
                        continue
                sql_condition = get_sql_condition_by_filter(column, filter_item)
                filter_string_list.append(sql_condition)
            if filter_string_list:
                filter_content = "(%s)" % (
                    filter_conjunction_split.join(filter_string_list)
                )
                group_string_list.append(filter_content)

        return "%s%s" % (
            filter_header,
            group_conjunction_split.join(group_string_list)
        )

    def to_sql(self, start=0, limit=500):
        sql = "%s %s" % (
            "SELECT * FROM",
            self.table_name
        )
        filter_clause = self._filter2sql()
        sort_clause = self._sort2sql()
        limit_clause = '%s %s, %s' % (
            "LIMIT",
            start,
            limit
        )
        return "%s %s %s %s" % (sql, filter_clause, sort_clause, limit_clause)