#
# file: migrations/0005.modify_user.py
#
from yoyo import step

__depends__ = {'0004.add_sis_id'}

steps = [
    step(
        '''
            ALTER TABLE user 
            DROP column workflow_state;
        '''
    ),
    step(
        '''
            ALTER TABLE user
            MODIFY
                uniqname
                VARCHAR(50) NULL;
        '''
    )
]