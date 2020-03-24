#
# file: migrations/0003.created_date_datatype_change.py
#
from yoyo import step

__depends__ = {'0002.add_col_published_date'}

step("ALTER TABLE course MODIFY column created_at DATETIME NULL")