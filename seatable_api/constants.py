from enum import Enum, unique


ROW_FILTER_KEYS = ['column_name', 'filter_predicate', 'filter_term', 'filter_term_modifier']
JOIN_ROOM = 'join-room'
UPDATE_DTABLE = 'update-dtable'
NEW_NOTIFICATION = 'new-notification'


##### column operations #####
RENAME_COLUMN = 'rename_column'
RESIZE_COLUMN = 'resize_column'
FREEZE_COLUMN = 'freeze_column'
MOVE_COLUMN = 'move_column'
MODIFY_COLUMN_TYPE = 'modify_column_type'
DELETE_COLUMN = 'delete_column'


##### column types #####
@unique
class ColumnTypes(Enum):
    DEFAULT = 'default'
    NUMBER = 'number'
    TEXT = 'text'
    CHECKBOX = 'checkbox'
    DATE = 'date'
    SINGLE_SELECT = 'single-select'
    LONG_TEXT = 'long-text'
    IMAGE = 'image'
    FILE = 'file'
    MULTIPLE_SELECT = 'multiple-select'
    COLLABORATOR = 'collaborator'
    LINK = 'link'
    FORMULA = 'formula'
    LINK_FORMULA = 'link-formula'
    CREATOR = 'creator'
    CTIME = 'ctime'
    LAST_MODIFIER = 'last-modifier'
    MTIME = 'mtime'
    GEOLOCATION = 'geolocation'
    AUTO_NUMBER = 'auto-number'
    URL = 'url'
    EMAIL = 'email'
    DURATION = 'duration'
    BUTTON = 'button'
    RATE = 'rate'

class FilterPredicateTypes(object):
    CONTAINS= 'contains'
    NOT_CONTAIN= 'does_not_contain'
    IS= 'is'
    IS_NOT= 'is_not'
    EQUAL= 'equal'
    NOT_EQUAL= 'not_equal'
    LESS= 'less'
    GREATER= 'greater'
    LESS_OR_EQUAL= 'less_or_equal'
    GREATER_OR_EQUAL= 'greater_or_equal'
    EMPTY= 'is_empty'
    NOT_EMPTY= 'is_not_empty'
    IS_WITHIN= 'is_within'
    IS_BEFORE= 'is_before'
    IS_AFTER= 'is_after'
    IS_ON_OR_BEFORE= 'is_on_or_before'
    IS_ON_OR_AFTER= 'is_on_or_after'
    HAS_ANY_OF= 'has_any_of'
    HAS_ALL_OF= 'has_all_of'
    HAS_NONE_OF= 'has_none_of'
    IS_EXACTLY= 'is_exactly'
    IS_ANY_OF= 'is_any_of'
    IS_NONE_OF= 'is_none_of'


class FilterTermModifier(object):
    TODAY = 'today'
    TOMORROW = 'tomorrow',
    YESTERDAY = 'yesterday'
    ONE_WEEK_AGO = 'one_week_ago'
    ONE_WEEK_FROM_NOW = 'one_week_from_now'
    ONE_MONTH_AGO = 'one_month_ago'
    ONE_MONTH_FROM_NOW = 'one_month_from_now'
    NUMBER_OF_DAYS_AGO = 'number_of_days_ago'
    NUMBER_OF_DAYS_FROM_NOW = 'number_of_days_from_now'
    EXACT_DATE = 'exact_date'
    THE_PAST_WEEK = 'the_past_week'
    THE_PAST_MONTH = 'the_past_month'
    THE_PAST_YEAR = 'the_past_year'
    THE_NEXT_WEEK = 'the_next_week'
    THE_NEXT_MONTH = 'the_next_month'
    THE_NEXT_YEAR = 'the_next_year'
    THE_NEXT_NUMBERS_OF_DAYS = 'the_next_numbers_of_days'
    THE_PAST_NUMBERS_OF_DAYS = 'the_past_numbers_of_days'
    THIS_WEEK = 'this_week'
    THIS_MONTH = 'this_month'
    THIS_YEAR = 'this_year'

class FormulaResultType(object):
    NUMBER = 'number'
    STRING = 'string'
    DATE = 'date'
    BOOL = 'bool'
    ARRAY = 'array'
