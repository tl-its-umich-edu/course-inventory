#
# file: migrations/0016.remove_user_table.py
#
from yoyo import step

__depends__ = {'0006.modify_user'}

steps = [
    step('''
        ALTER TABLE enrollment
            DROP FOREIGN KEY fk_user_id;
    '''),
    step('''
        DROP TABLE user;
    ''')
]
