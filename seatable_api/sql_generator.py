from seatable_api.constants import FilterPredicateTypes, ColumnTypes, FilterTermModifier, FormulaResultType
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class Operator(object):

    def __init__(self, column, filter_item):
        self.column = column
        self.filter_item = filter_item

        self.column_name = ''
        self.filter_term = ''

        self.filter_predicate = ''
        self.filter_term_modifier = ''
        self.column_type = ''

        self.init()

    def init(self):
        self.column_name = self.column.get('name', '')
        self.column_type = self.column.get('type', '')
        self.filter_predicate = self.filter_item.get('filter_predicate', '')
        self.filter_term = self.filter_item.get('filter_term', '')
        self.filter_term_modifier = self.filter_item.get('filter_term_modifier', '')

    def op_is(self):
        return "%s %s '%s'" % (
            self.column_name,
            '=',
            self.filter_term
        )

    def op_is_not(self):
        return "%s %s '%s'" % (
            self.column_name,
            '<>',
            self.filter_term
        )

    def op_contains(self):
        return "%s %s '%%%s%%'" % (
            self.column_name,
            'like',
            self.filter_term
        )

    def op_does_not_contain(self):
        return "%s %s '%%%s%%'" % (
            self.column_name,
            'not like',
            self.filter_term
        )

    def op_equal(self):
        return "%(column_name)s = %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_not_equal(self):
        return "%(column_name)s <> %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_less(self):
        return "%(column_name)s < %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_less_or_equal(self):
        return "%(column_name)s <= %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_greater(self):
        return "%(column_name)s > %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_greater_or_equal(self):
        return "%(column_name)s >= %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_is_empty(self):
        return "%(column_name)s is null" % ({
            'column_name': self.column_name
        })

    def op_is_not_empty(self):
        return "%(column_name)s is not null" % ({
            'column_name': self.column_name
        })

    def op_is_exactly(self):
        pass

    def op_is_any_of(self):
        pass

    def op_is_none_of(self):
        pass

    def op_is_on_or_after(self):
        pass

    def op_is_on_or_before(self):
        pass

    def op_has_all_of(self):
        pass

    def op_has_any_of(self):
        pass

    def op_has_none_of(self):
        pass

    def op_is_before(self):
        pass

    def op_is_after(self):
        pass

    def get_related_operator(self):
        pass


class TextOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.CONTAINS,
        FilterPredicateTypes.NOT_CONTAIN,
        FilterPredicateTypes.IS,
        FilterPredicateTypes.IS_NOT,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
    ]

    def __init__(self, column, filter_item):
        super(TextOperator, self).__init__(column, filter_item)


class NumberOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.EQUAL,
        FilterPredicateTypes.NOT_EQUAL,
        FilterPredicateTypes.GREATER,
        FilterPredicateTypes.GREATER_OR_EQUAL,
        FilterPredicateTypes.LESS,
        FilterPredicateTypes.LESS_OR_EQUAL,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
    ]

    def __init__(self, column, filter_item):
        super(NumberOperator, self).__init__(column, filter_item)


class SingleSelectOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.IS_ANY_OF,
        FilterPredicateTypes.IS_NONE_OF,
        FilterPredicateTypes.IS,
        FilterPredicateTypes.IS_NOT,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
    ]

    def __init__(self, column, filter_item):
        super(SingleSelectOperator, self).__init__(column, filter_item)

    def op_is_any_of(self):
        filter_term = self.filter_item
        if not isinstance(filter_term, list):
            filter_term = [filter_term, ]
        option_names = ["'%s'" % (op_name) for op_name in filter_term]
        return "%(column_name)s in (%(option_names)s)" % ({
            "column_name": self.column_name,
            "option_names": ", ".join(option_names)
        })

    def op_is_none_of(self):
        filter_term = self.filter_item
        if not isinstance(filter_term, list):
            filter_term = [filter_term, ]
        option_names = ["'%s'" % (op_name) for op_name in filter_term]
        return "%(column_name)s not in (%(option_names)s)" % ({
            "column_name": self.column_name,
            "option_names": ", ".join(option_names)
        })


class MultipleSelectOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.HAS_ANY_OF,
        FilterPredicateTypes.HAS_NONE_OF,
        FilterPredicateTypes.HAS_ALL_OF,
        FilterPredicateTypes.IS_EXACTLY,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
    ]

    def __init__(self, column, filter_item):
        super(MultipleSelectOperator, self).__init__(column, filter_item)
        self.option_name_str = ''

    def op_has_any_of(self):
        option_names = ["'%s'" % op_name for op_name in self.filter_term]
        option_names_str = ', '.join(option_names)
        return "%(column_name)s in (%(option_names_str)s)" % ({
            "column_name": self.column_name,
            "option_names_str": option_names_str
        })

    def op_has_none_of(self):
        option_names = ["'%s'" % op_name for op_name in self.filter_term]
        option_names_str = ', '.join(option_names)
        return "%(column_name)s has none of (%(option_names_str)s)" % ({
            "column_name": self.column_name,
            "option_names_str": option_names_str
        })

    def op_has_all_of(self):
        option_names = ["'%s'" % op_name for op_name in self.filter_term]
        option_names_str = ', '.join(option_names)
        return "%(column_name)s has all of (%(option_names_str)s)" % ({
            "column_name": self.column_name,
            "option_names_str": option_names_str
        })

    def op_is_exactly(self):
        option_names = ["'%s'" % op_name for op_name in self.filter_term]
        option_names_str = ', '.join(option_names)
        return "%(column_name)s is exactly (%(option_names_str)s)" % ({
            "column_name": self.column_name,
            "option_names_str": option_names_str
        })


class DateOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.IS,
        FilterPredicateTypes.IS_NOT,
        FilterPredicateTypes.IS_AFTER,
        FilterPredicateTypes.IS_BEFORE,
        FilterPredicateTypes.IS_ON_OR_BEFORE,
        FilterPredicateTypes.IS_ON_OR_AFTER,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
        FilterPredicateTypes.IS_WITHIN,
    ]


    def __init__(self, column, filter_items):
        super(DateOperator, self).__init__(column, filter_items)

    def _get_end_day_of_month(self, year, month):
        days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            days[1] = 29

        return days[month - 1]

    def _format_date(self, dt):
        return dt.strftime("%Y-%m-%d")

    def _other_date(self):
        filter_term_modifier = self.filter_term_modifier
        filter_term = self.filter_term
        today = datetime.today()
        year = today.year

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
            week_day = today.isoweekday() + 1  # 1-7
            start_date = today - timedelta(days=(week_day + 6))
            end_date = today - timedelta(days=week_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THIS_WEEK:
            week_day = today.isoweekday() + 1
            start_date = today - timedelta(days=week_day - 1)
            end_date = today + timedelta(days=7 - week_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THE_NEXT_WEEK:
            week_day = today.isoweekday() + 1
            start_date = today + timedelta(days=7 - week_day)
            end_date = today + timedelta(days=14 - week_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THE_PAST_MONTH:
            one_month_ago = today - relativedelta(months=1)
            one_month_ago_year = one_month_ago.year
            one_month_ago_month = one_month_ago.month
            one_month_age_end_day = self._get_end_day_of_month(one_month_ago_year, one_month_ago_month)
            start_date = datetime(one_month_ago_year, one_month_ago_month, 1)
            end_date = datetime(one_month_ago_year, one_month_ago_month, one_month_age_end_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THIS_MONTH:
            current_month = today.month
            current_year = today.year
            current_month_end_day = self._get_end_day_of_month(current_year, current_month)
            start_date = datetime(current_year, current_month, 1)
            end_date = datetime(current_year, current_month, current_month_end_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THE_NEXT_MONTH:
            next_month = today + relativedelta(months=1)
            next_month_year = next_month.year
            next_month_month = next_month.month
            next_month_end_day = self._get_end_day_of_month(next_month_year, next_month_month)
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

    def op_is(self):
        date = self._other_date()
        next_date = self._format_date(date + timedelta(days=1))
        target_date = self._format_date(date)
        return "%(column_name)s >= '%(target_date)s' and %(column_name)s < '%(next_date)s'" % ({
            "column_name": self.column_name,
            "target_date": target_date,
            "next_date": next_date
        })

    def op_is_within(self):
        start_date, end_date = self._other_date()
        return "%(column_name)s >= '%(start_date)s' and %(column_name)s < '%(end_date)s'" % ({
            "column_name": self.column_name,
            "start_date": self._format_date(start_date),
            "end_date": self._format_date(end_date)
        })

    def op_is_before(self):
        target_date = self._other_date()
        return "%(column_name)s < '%(target_date)s' and %(column_name)s is not null" % ({
            "column_name": self.column_name,
            "target_date": self._format_date(target_date)
        })

    def op_is_after(self):
        target_date = self._other_date()
        return "%(column_name)s > '%(target_date)s'" % ({
            "column_name": self.column_name,
            "target_date": self._format_date(target_date)
        })

    def op_is_on_or_before(self):
        target_date = self._other_date()
        return "%(column_name)s <= '%(target_date)s' and %(column_name)s is not null" % ({
            "column_name": self.column_name,
            "target_date": self._format_date(target_date)
        })

    def op_is_on_or_after(self):
        target_date = self._other_date()
        return "%(column_name)s >= '%(target_date)s' and %(column_name)s is not null" % ({
            "column_name": self.column_name,
            "target_date": self._format_date(target_date)
        })

    def op_is_not(self):
        target_date = self._other_date()
        start_date = target_date - timedelta(days=1)
        end_date = target_date + timedelta(days=1)
        return "(%(column_name)s >= '%(end_date)s' or %(column_name)s <= '%(start_date)s') and %(column_name)s is not null" % (
        {
            "column_name": self.column_name,
            "start_date": self._format_date(start_date),
            "end_date": self._format_date(end_date)
        })


class CheckBoxOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.IS,
    ]

    def op_is(self):
        return "%(column_name)s = %(value)s" % ({
            "column_name": self.column_name,
            "value": self.filter_term
        })


class CreatorOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.CONTAINS,
        FilterPredicateTypes.NOT_CONTAIN,
        FilterPredicateTypes.IS,
        FilterPredicateTypes.IS_NOT,
    ]

    def op_contains(self):
        select_collaborators = self.filter_term
        if not isinstance(select_collaborators, list):
            select_collaborators = [select_collaborators, ]
        creator_list = ["'%s'" % collaborator for collaborator in select_collaborators]
        filter_term_str = ", ".join(creator_list)
        return "%(column_name)s in (%(filter_term_str)s)" % ({
            "column_name": self.column_name,
            "filter_term_str": filter_term_str
        })

    def op_does_not_contain(self):
        select_collaborators = self.filter_term
        if not isinstance(select_collaborators, list):
            select_collaborators = [select_collaborators, ]
        creator_list = ["'%s'" % collaborator for collaborator in select_collaborators]
        sql_slice = []
        for creator in creator_list:
            sql_slice.append("%(column_name)s != %(creator)s" % ({
                "column_name": self.column_name,
                "creator": creator
            }))
        return ' and '.join(sql_slice)


class FormularOperator(Operator):

    def __init__(self, column, filter_item):
        super(FormularOperator, self).__init__(column, filter_item)

    def get_related_operator(self):
        result_type = self.column.get('data', {}).get('result_type')
        if result_type == FormulaResultType.NUMBER:
            return NumberOperator(self.column, self.filter_item)
        if result_type == FormulaResultType.DATE:
            return DateOperator(self.column, self.filter_item)
        if result_type == FormulaResultType.BOOL:
            return CheckBoxOperator(self.column, self.filter_item)

        return TextOperator(self.column, self.filter_item)


def _filter2sqlslice(operator):
    support_fitler_predicates = operator.SUPPORT_FILTER_PREDICATE
    if not operator.filter_predicate in support_fitler_predicates:
        raise ValueError(
            "%(column_type)s type column does not support '%(value)s, available predicates are %(available_predicates)s" % (
            {
                'column_type': operator.column_type,
                'value': operator.filter_predicate,
                'available_predicates': support_fitler_predicates,
            })
        )
    return {
        FilterPredicateTypes.IS: operator.op_is(),
        FilterPredicateTypes.IS_NOT: operator.op_is_not(),
        FilterPredicateTypes.CONTAINS: operator.op_contains(),
        FilterPredicateTypes.NOT_CONTAIN: operator.op_does_not_contain(),
        FilterPredicateTypes.NOT_EMPTY: operator.op_is_not_empty(),
        FilterPredicateTypes.IS_EXACTLY: operator.op_is_exactly(),
        FilterPredicateTypes.EMPTY: operator.op_is_empty(),
        FilterPredicateTypes.EQUAL: operator.op_equal(),
        FilterPredicateTypes.NOT_EQUAL: operator.op_not_equal(),
        FilterPredicateTypes.GREATER: operator.op_greater(),
        FilterPredicateTypes.GREATER_OR_EQUAL: operator.op_greater_or_equal(),
        FilterPredicateTypes.LESS: operator.op_less(),
        FilterPredicateTypes.LESS_OR_EQUAL: operator.op_less_or_equal(),
        FilterPredicateTypes.IS_ANY_OF: operator.op_is_any_of(),
        FilterPredicateTypes.IS_NONE_OF: operator.op_is_none_of(),
        FilterPredicateTypes.IS_ON_OR_AFTER: operator.op_is_on_or_after(),
        FilterPredicateTypes.IS_ON_OR_BEFORE: operator.op_is_on_or_before(),
        FilterPredicateTypes.HAS_ALL_OF: operator.op_has_all_of(),
        FilterPredicateTypes.HAS_ANY_OF: operator.op_has_any_of(),
        FilterPredicateTypes.HAS_NONE_OF: operator.op_has_none_of(),
        FilterPredicateTypes.IS_BEFORE: operator.op_is_before(),
        FilterPredicateTypes.IS_AFTER: operator.op_is_after(),

    }.get(operator.filter_predicate, '')

def _get_operator_by_type(column_type):

    return {
        ColumnTypes.TEXT.value: TextOperator,
        ColumnTypes.URL.value: TextOperator,
        ColumnTypes.AUTO_NUMBER.value: TextOperator,
        ColumnTypes.EMAIL.value: TextOperator,
        ColumnTypes.GEOLOCATION.value: TextOperator,

        ColumnTypes.DURATION.value: NumberOperator,
        ColumnTypes.NUMBER.value: NumberOperator,
        ColumnTypes.RATE.value: NumberOperator,

        ColumnTypes.CHECKBOX.value: CheckBoxOperator,

        ColumnTypes.DATE.value: DateOperator,
        ColumnTypes.CTIME.value: DateOperator,
        ColumnTypes.MTIME.value: DateOperator,

        ColumnTypes.SINGLE_SELECT.value: SingleSelectOperator,

        ColumnTypes.MULTIPLE_SELECT.value: MultipleSelectOperator,
        ColumnTypes.COLLABORATOR.value: MultipleSelectOperator,
        ColumnTypes.LINK.value: MultipleSelectOperator,

        ColumnTypes.CREATOR.value: CreatorOperator,
        ColumnTypes.LAST_MODIFIER.value: CreatorOperator,

        ColumnTypes.FORMULA.value: FormularOperator,
        ColumnTypes.LINK_FORMULA.value: FormularOperator

    }.get(column_type, None)


class BaseSQLGenerator(object):

    def __init__(self, authed_base, table_name, filter_conditions=None, filter_condition_groups=None):
        self.base = authed_base
        self.table_name = table_name
        self.filter_conditions = filter_conditions
        self.filter_condition_groups = filter_condition_groups
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

    def _sort2sql(self, by_group=False):
        if by_group:
            filter_conditions = self.filter_condition_groups
        else:
            filter_conditions = self.filter_conditions
        condition_sorts = filter_conditions.get('sorts', [])
        if not condition_sorts:
            return ''

        order_header = 'ORDER BY '
        clauses = []
        for sort in condition_sorts:
            column_key = sort.get('column_key', '')
            column_name = sort.get('column_name', '')
            sort_type = sort.get('sort_type', 'DESC') == 'up' and 'ASC' or 'DESC'
            column = self._get_column_by_key(column_key)
            if not column:
                column = self._get_column_by_name(column_name)
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

    def _groupfilter2sql(self):
        filter_condition_groups = self.filter_condition_groups
        filter_groups = filter_condition_groups.get('filter_groups', [])
        group_conjunction = filter_condition_groups.get('group_conjunction', 'And')
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

                column_type = column.get('type')
                operator = _get_operator_by_type(column_type)(column, filter_item)
                if column_type in [ColumnTypes.LINK_FORMULA.value, ColumnTypes.FORMULA.value]:
                    operator = operator.get_related_operator()
                if not operator:
                    raise ValueError('%s type column does not surpport transfering filter to sql' % column_type)
                sql_condition = _filter2sqlslice(operator)
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

    def _filter2sql(self):
        filter_conditions = self.filter_conditions
        filters = filter_conditions.get('filters', [])
        filter_conjunction = filter_conditions.get('filter_conjunction', 'And')
        if not filters:
            return ''

        filter_header = 'WHERE '
        filter_string_list = []
        filter_content = ''
        filter_conjunction_split = " %s " % filter_conjunction
        for filter_item in filters:
            column_key = filter_item.get('column_key')
            column_name = filter_item.get('column_name')
            column = column_key and self._get_column_by_key(column_key)
            if not column:
                column = column_name and self._get_column_by_name(column_name)
                if not column:
                    continue
            column_type = column.get('type')
            operator = _get_operator_by_type(column_type)(column, filter_item)
            if column_type in [ColumnTypes.LINK_FORMULA.value, ColumnTypes.FORMULA.value]:
                operator = operator.get_related_operator()
            if not operator:
                raise ValueError('%s type column does not surpport transfering filter to sql' % column_type)
            sql_condition = _filter2sqlslice(operator)
            filter_string_list.append(sql_condition)
        if filter_string_list:
            filter_content = "%s" % (
                filter_conjunction_split.join(filter_string_list)
            )
        return "%s%s" % (
            filter_header,
            filter_content
        )

    def to_sql(self, start=0, limit=500, by_group=False):
        sql = "%s %s" % (
            "SELECT * FROM",
            self.table_name
        )
        if not by_group:
            filter_clause = self._filter2sql()
            sort_clause = self._sort2sql()
        else:
            filter_clause = self._groupfilter2sql()
            sort_clause = self._sort2sql(by_group=True)
        limit_clause = '%s %s, %s' % (
            "LIMIT",
            start,
            limit
        )
        if filter_clause:
            sql = "%s %s" % (sql, filter_clause)
        if sort_clause:
            sql = "%s %s" % (sql, sort_clause)
        if limit_clause:
            sql = "%s %s" % (sql, limit_clause)
        return sql


def filter2sql(authed_base, table_name, filter_conditions, start=0, limit=500):
    sql_generator = BaseSQLGenerator(authed_base, table_name, filter_conditions=filter_conditions)
    return sql_generator.to_sql(start=start, limit=limit)
