import copy

from ply import lex, yacc


ply_tokens = (
    'SELECT',
    'WHERE',
    'AND',
    'OR',

    'TERMINATOR',
    'ALL_COLUMNS',
    'SPLIT',
    'LBORDER',
    'RBORDER',

    'EQUAL',
    'GTE',
    'GT',
    'LTE',
    'LT',

    'QUOTE_STRING',
    'STRING',
)


class Lexer(object):

    def __init__(self):
        self.lexer = lex.lex(module=self)

    def check(self, text):
        if not text:
            return
        self.lexer.input(text)

    # List of token names. This is always required
    tokens = ply_tokens

    reserved = {
        'select': 'SELECT',
        'SELECT': 'SELECT',
        'Select': 'SELECT',
        'where': 'WHERE',
        'WHERE': 'WHERE',
        'Where': 'WHERE',
        'and': 'AND',
        'AND': 'AND',
        'And': 'AND',
        'or': 'OR',
        'OR': 'OR',
        'Or': 'OR',
    }
    # t_SELECT = r'(select|SELECT|Select)'
    # t_WHERE = r'(where|WHERE|Where)'
    # t_AND = r'(and|AND|And)'
    # t_OR = r'(or|OR|Or)'

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


# Lexer instance
ply_lexer = Lexer()


class SelectParser(object):

    def __init__(self):
        self.yaccer = yacc.yacc(module=self)

    # Parse
    def parse(self, raw_rows, raw_columns_name_list, select_text):
        self.selected_columns = []
        self.filtered_rows = []
        self.raw_rows = copy.deepcopy(raw_rows)
        self.raw_columns_name_list = raw_columns_name_list

        # parse selected_columns
        self.yaccer.parse(select_text, lexer=self.lexer)
        self._filter()
        return self.filtered_rows

    def _check_selected_columns(self):
        self.selected_columns = list(set(self.selected_columns))
        for column_name in self.selected_columns:
            if column_name not in self.raw_columns_name_list:
                raise ValueError('Column not found!', column_name)

    def _filter(self):
        if '*' in self.selected_columns or not self.selected_columns:
            self.filtered_rows = self.raw_rows
        else:
            self._check_selected_columns()
            filtered_rows = []
            for row in self.raw_rows:
                data = {'_id': row['_id']}
                for column in self.selected_columns:
                    data[column] = row.get(column)
                filtered_rows.append(data)
            self.filtered_rows = filtered_rows

    lexer = ply_lexer.lexer

    # List of token names. This is always required
    tokens = ply_tokens

    # Do nothing
    def p_column_select(self, p):
        """expression : SELECT column
        """
        pass

    # Do nothing
    def p_column_split(self, p):
        """column : column SPLIT column
        """
        pass

    # Parse selected columns
    def p_column(self, p):
        """column : ALL_COLUMNS
                  | QUOTE_STRING
                  | STRING
        """
        self.selected_columns.append(p[1])

    # Error rule for syntax errors
    def p_error(self, p):
        raise ValueError('Syntax error in input!', p.value)


# SelectParser instance
select_parser = SelectParser()


class QuerySet(object):

    def __init__(self, raw_rows, raw_columns, sql):
        self.rows = raw_rows
        self.raw_rows = raw_rows
        self.raw_columns = raw_columns
        self.raw_columns_name_list = [column['name'] for column in raw_columns]
        self._execute(sql)

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def _execute(self, sql):
        if sql and self.rows:
            where_index = sql.lower().index('where')
            select_text = sql[:where_index]
            where_text = sql[where_index:]
            rows = select_parser.parse(self.rows, self.raw_columns_name_list, select_text)
            self.rows = rows
        else:
            pass

    def filter(self, sql=None):
        self._execute(sql)

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
