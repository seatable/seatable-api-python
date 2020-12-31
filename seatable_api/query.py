import copy

from ply import lex, yacc


class Lexer(object):

    def __init__(self):
        self.lexer = lex.lex(module=self)

    # List of token names. This is always required
    tokens = (
        'SELECT', 'WHERE',
        'TERMINATOR', 'ALL_COLUMNS', 'SPLIT',
        'LBORDER', 'RBORDER',
        'AND', 'OR',
        'EQUAL', 'GTE', 'GT', 'LTE', 'LT',
        'QUOTE_STRING', 'STRING',
    )

    reserved = {
        'select': 'SELECT', 'SELECT': 'SELECT', 'Select': 'SELECT',
        'where': 'WHERE', 'WHERE': 'WHERE', 'Where': 'WHERE',
        'and': 'AND', 'AND': 'AND', 'And': 'AND',
        'or': 'OR', 'OR': 'OR', 'Or': 'OR',
    }

    # Regular expression rules for simple tokens
    t_TERMINATOR = r';'
    t_ALL_COLUMNS = r'\*'
    t_SPLIT = r','
    t_LBORDER = r'\('
    t_RBORDER = r'\)'

    t_EQUAL = r'='
    t_GTE = r'>='
    t_GT = r'>'
    t_LTE = r'<='
    t_LT = r'<'

    def t_QUOTE_STRING(self, t):
        r"'([^']*)'"
        t.value = t.value[1:-1]
        return t

    def t_STRING(self, t):
        r'[^=|>|<|\*|/(|/)|\s|\^|\/|;|,]+'
        t.type = self.reserved.get(t.value, 'STRING')
        return t

    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)

    # A string containing ignored characters (spaces and tabs)
    t_ignore = ' \t'
    # t_ignore_COMMENT = r'\#'

    # Error handling rule
    def t_error(self, t):
        raise ValueError('Illegal character!', t.value[0])


class WhereParser(object):

    def __init__(self):
        self.yaccer = yacc.yacc(module=self)

    # Parse
    def parse(self, raw_rows, raw_columns_map, where_text):
        self.raw_rows = copy.deepcopy(raw_rows)
        self.raw_columns_map = raw_columns_map
        # main
        rows = self.yaccer.parse(where_text, lexer=self.lexer)
        return rows

    def _check_column_exists(self, column):
        if column not in self.raw_columns_map:
            raise ValueError('Column not found!', column)

    def _exchange_value(self, column_type, value):
        if column_type == 'number':
            if '.' in value:
                value = float(value)
            else:
                value = int(value)
        return value

    def _merge(self, left_rows, condition, right_rows):
        merged_rows = []
        left_rows_id_list = [row['_id'] for row in left_rows]

        if condition.lower() == 'and':
            for row in right_rows:
                if row['_id'] in left_rows_id_list:
                    merged_rows.append(row)

        elif condition.lower() == 'or':
            merged_rows = left_rows
            for row in right_rows:
                if row['_id'] not in left_rows_id_list:
                    merged_rows.append(row)

        return merged_rows

    def _filter(self, column, condition, value):
        self._check_column_exists(column)
        filtered_rows = []
        column_type = self.raw_columns_map[column].get('type')
        value = self._exchange_value(column_type, value)

        if condition == '=':
            for row in self.raw_rows:
                if row.get(column) == value:
                    filtered_rows.append(row)

        elif condition == '>=':
            for row in self.raw_rows:
                if row.get(column) >= value:
                    filtered_rows.append(row)

        elif condition == '>':
            for row in self.raw_rows:
                if row.get(column) > value:
                    filtered_rows.append(row)

        elif condition == '<=':
            for row in self.raw_rows:
                if row.get(column) <= value:
                    filtered_rows.append(row)

        elif condition == '<':
            for row in self.raw_rows:
                if row.get(column) < value:
                    filtered_rows.append(row)

        return filtered_rows

    # Lexer
    lexer = Lexer().lexer

    # List of token names. This is always required
    tokens = (
        'AND', 'OR',
        'EQUAL', 'GTE', 'GT', 'LTE', 'LT',
        'QUOTE_STRING', 'STRING',
    )

    def p_merge(self, p):
        """merge : filter AND filter
                 | filter OR filter
                 | merge AND filter
                 | merge OR filter
                 | filter
        """
        if len(p.slice) > 2:
            p[0] = self._merge(p[1], p[2], p[3])
        else:
            p[0] = p[1]

    def p_filter(self, p):
        """filter : factor EQUAL factor
                  | factor GTE factor
                  | factor GT factor
                  | factor LTE factor
                  | factor LT factor
        """
        p[0] = self._filter(p[1], p[2], p[3])

    def p_factor(self, p):
        """factor : QUOTE_STRING
                  | STRING
        """
        p[0] = p[1]

    # Error rule for syntax errors
    def p_error(self, p):
        raise ValueError('Syntax error in input!', p.value)


class SelectParser(object):

    # Parse
    def parse(self, raw_rows, raw_columns_map, select_text):
        self.raw_rows = copy.deepcopy(raw_rows)
        self.raw_columns_map = raw_columns_map
        # main
        selected_columns = self._parse_select_text(select_text)
        rows, columns = self._modify(selected_columns)
        return rows, columns

    def _parse_select_text(self, select_text):
        selected_columns = []
        for column in select_text.split(','):
            column = column.strip(' ').strip("'")
            if column:
                selected_columns.append(column)
        return selected_columns

    def _check_columns_exists(self, selected_columns):
        for column in selected_columns:
            if column not in self.raw_columns_map:
                raise ValueError('Column not found!', column)

    def _modify(self, selected_columns):
        if '*' in selected_columns or not selected_columns:
            return self.raw_rows, self.raw_columns_map.values()
        else:
            self._check_columns_exists(selected_columns)
            modified_rows = []
            modified_columns = [
                self.raw_columns_map[column]for column in selected_columns]
            for row in self.raw_rows:
                data = {'_id': row['_id']}
                for column in selected_columns:
                    data[column] = row.get(column)
                modified_rows.append(data)
            return modified_rows, modified_columns


class QuerySet(object):

    def __init__(self, raw_rows, raw_columns, sql):
        self.raw_rows = raw_rows
        self.raw_columns = raw_columns
        self.sql = sql
        self.rows = []
        self.columns = []
        self._execute(sql)

    def _execute_sql(self, raw_rows, raw_columns, sql):
        columns_map = {column['name']: column for column in raw_columns}
        where_index = sql.lower().index('where')
        where_text = sql[where_index + len('where'):].rstrip(';')
        select_text = sql[len('select'): where_index]
        # main
        rows = WhereParser().parse(raw_rows, columns_map, where_text)
        rows, columns = SelectParser().parse(rows, columns_map, select_text)
        return rows, columns

    def _execute(self, sql, clone=None):
        if not clone:
            rows, columns = self.raw_rows, self.raw_columns
            if sql and self.raw_rows and self.raw_columns:
                rows, columns = self._execute_sql(
                    self.raw_rows, self.raw_columns, sql)
            self.rows = rows
            self.columns = columns
        else:
            rows, columns = clone.raw_rows, clone.raw_columns
            if sql and clone.raw_rows and clone.raw_columns:
                rows, columns = self._execute_sql(
                    clone.raw_rows, clone.raw_columns, sql)
            clone.rows = rows
            clone.columns = columns

    def _clone(self, sql):
        clone = self.__class__(self.rows, self.columns, sql)
        return clone

    def __str__(self):
        return '<SeaTable Queryset "' + str(self.sql) + '">'

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def filter(self, sql=None):
        clone = self._clone(sql)
        self._execute(sql, clone)
        return clone

    def first(self):
        if self.rows:
            return self.rows[0]
        else:
            return None

    def last(self):
        if self.rows:
            return self.rows[-1]
        else:
            return None

    def count(self):
        return len(self.rows)

    def exists(self):
        return len(self.rows) > 0
