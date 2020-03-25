#
# file: migrations/0004.add_um_id.py
#
from yoyo import step

__depends__ = {'0003.created_date_datatype_change'}

step("ALTER TABLE user ADD column um_id INTEGER NULL AFTER name;")
