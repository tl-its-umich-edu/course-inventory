#
# file: migrations/0002.add_col_published_date.py
#
from yoyo import step

__depends__ = {'0001.init_inventory_db'}

step("ALTER TABLE course ADD column published_at DATETIME NULL AFTER created_at")