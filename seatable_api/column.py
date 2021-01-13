from seatable_api.constants import ColumnTypes
import datetime

class ColumnValue(object):
    """
    This is for the computation of the comparison between the input value and cell value from table
    such as >, <, =, >=, <=, !=, which is supposed to fit different column types
    """
    def __init__(self, column_value, column_type):
        self.column_value = column_value
        self.column_type = column_type

    def equal(self,value):
        return self.column_value == value

    def unequal(self, value):
        return self.column_value != value

    def greater_equal(self, value):
        raise ValueError("%s type column does not support the query method '%s'" % (self.column_type, '>='))

    def greater(self, value):
        raise ValueError("%s type column does not support the query method '%s'" % (self.column_type, '>'))

    def less_equal(self, value):
        raise ValueError("%s type column does not support the query method '%s'" % (self.column_type, '<='))

    def less(self, value):
        raise ValueError("%s type column does not support the query method '%s'" % (self.column_type, '<'))

    def like(self, value):
        '''fuzzy search'''
        raise ValueError("%s type column does not support the query method '%s'" % (self.column_type, 'like'))


class StringColumnValue(ColumnValue):
    """
    the return data of string column value is type of string, including cloumn type of
    text, creator, single-select, url, email,....., and support the computation of
    = ,!=, and like(fuzzy search)
    """
    def like(self, value):
        if "%" in value:
            column_value = self.column_value
            # 1. abc% pattern, start with abc
            if value[0] != '%' and value[-1] == '%':
                start = value[:-1]
                return column_value.startswith(start)

            # 2. %abc pattern, end with abc
            elif value[0] == '%' and value[-1] != '%':
                end = value[1:]
                return column_value.endswith(end)

            # 3. %abc% pattern, contains abc
            elif value[0] == '%' and value[-1] == '%':
                middle = value[1:-1]
                return middle in column_value

            # 4. a%b pattern, start with a and end with b
            else:
                value_split_list = value.split('%')
                start = value_split_list[0]
                end = value_split_list[-1]
                return column_value.startswith(start) and column_value.endswith(end)

        else:
            raise ValueError('There is no patterns found in "like" phrases')

class NumberDateColumnValue(ColumnValue):
    """
    the returned data of number-date-column is digit number, or datetime obj, including the
    type of number, ctime, date, mtime, support the computation of =, > ,< ,>=, <=, !=
    """
    def greater_equal(self, value):
        return self.column_value >= value

    def greater(self, value):
        return self.column_value > value

    def less_equal(self, value):
        return self.column_value <= value

    def less(self, value):
        return self.column_value < value

class ListColumnValue(ColumnValue):
    """
    the returned data of list-column value is a list like datastructure, including the
    type of multiple-select, image, collaborator and so on, support the computation of
    =, != which should be decided by in or not in expression
    """
    def equal(self,value):
        return value in self.column_value

    def unequal(self, value):
        return value not in self.column_value



class TextColumn(object):

    def __init__(self):
        self.column_type = ColumnTypes.TEXT.value

    def parse_input_value(self, value):
        return value

    def parse_table_value(self, value):
        return StringColumnValue(value, self.column_type)

class NumberColumn(TextColumn):

    def __init__(self):
        super(NumberColumn, self).__init__()
        self.column_type = ColumnTypes.NUMBER.value

    def __str__(self):
        return "SeaTable Number Column"

    def parse_input_value(self, value):
        if '.' in value:
            value = float(value)
        else:
            try:
                value = int(value)
            except:
                value = 0
        return value

    def parse_table_value(self, value):
        return NumberDateColumnValue(value, self.column_type)

class DateColumn(TextColumn):

    def __init__(self):
        super(DateColumn, self).__init__()
        self.column_type = ColumnTypes.DATE.value

    def __str__(self):
        return "SeaTable Date Column"

    def parse_input_value(self, time_str):
        time_str_list = time_str.split(' ')
        datetime_obj = None
        if len(time_str_list) == 1:
            ymd = time_str_list[0]
            datetime_obj = datetime.datetime.strptime(ymd, '%Y-%m-%d')
        elif len(time_str_list) == 2:
            h, m, s = 0, 0, 0
            ymd, hms_str = time_str_list
            hms_str_list = hms_str.split(':')
            if len(hms_str_list) == 1:
                h = hms_str_list[0]
            elif len(hms_str_list) == 2:
                h, m = hms_str_list
            elif len(hms_str_list) == 3:
                h, m, s = hms_str_list
            datetime_obj = datetime.datetime.strptime("%s %s" % (
                ymd, "%s:%s:%s" % (h, m, s)), '%Y-%m-%d %H:%M:%S')
        return datetime_obj

    def parse_table_value(self, time_str):
        return NumberDateColumnValue(self.parse_input_value(time_str), self.column_type)

class CTimeColumn(DateColumn):

    def __init__(self):
        super(CTimeColumn, self).__init__()
        self.column_type = ColumnTypes.CTIME.value

    def __str__(self):
        return "SeaTable CTime Column"

    def parse_table_value(self, time_str):
        utc_time = datetime.datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S.%f+00:00')
        delta2utc = datetime.datetime.now() - datetime.datetime.utcnow()
        local_time = utc_time + delta2utc
        return NumberDateColumnValue(local_time, self.column_type)

class MTimeColumn(CTimeColumn):

    def __init__(self):
        super(MTimeColumn, self).__init__()
        self.column_type = ColumnTypes.MTIME.value

    def __str__(self):
        return "SeaTable MTime Column"

    def parse_table_value(self, time_str):
        utc_time = datetime.datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S.%f+00:00')
        delta2utc = datetime.datetime.now() - datetime.datetime.utcnow()
        local_time = utc_time + delta2utc
        return NumberDateColumnValue(local_time, self.column_type)

class CheckBoxColumn(TextColumn):

    def __init__(self):
        super(CheckBoxColumn, self).__init__()
        self.column_type = ColumnTypes.CHECKBOX.value

    def __str__(self):
        return "SeaTable Checkbox Column"

    def parse_input_value(self, value):
        if value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False

    def parse_table_value(self, value):
        return StringColumnValue(bool(value), self.column_type)

class MultiSelectColumn(TextColumn):

    def __init__(self):
        super(MultiSelectColumn, self).__init__()
        self.column_type = ColumnTypes.MULTIPLE_SELECT.value

    def parse_table_value(self, value):
        return ListColumnValue(value, self.column_type)



COLUMN_MAP = {
    ColumnTypes.NUMBER.value: NumberColumn(),
    ColumnTypes.DATE.value: DateColumn(),
    ColumnTypes.CTIME.value: CTimeColumn(),
    ColumnTypes.MTIME.value: MTimeColumn(),
    ColumnTypes.CHECKBOX.value: CheckBoxColumn(),
    ColumnTypes.TEXT.value: TextColumn(),
    ColumnTypes.MULTIPLE_SELECT.value: MultiSelectColumn(),
}

class Column(object):

    def __init__(self, column_type):
        self.column = COLUMN_MAP.get(column_type, TextColumn())

    def rasie_error(self, value):
        raise ValueError("%s type column does not support the query string as %s" % (self.column.column_type, value))

    def parse_input_value(self, value):
        try:
            return self.column.parse_input_value(value)
        except:
            self.rasie_error(value)

    def parse_table_value(self, value):
        try:
            return self.column.parse_table_value(value)
        except:
            self.rasie_error(value)