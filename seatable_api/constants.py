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
    CREATOR = 'creator'
    CTIME = 'ctime'
    LAST_MODIFIER = 'last-modifier'
    MTIME = 'mtime'
    GEOLOCATION = 'geolocation'
    AUTO_NUMBER = 'auto-number'
    URL = 'url'
