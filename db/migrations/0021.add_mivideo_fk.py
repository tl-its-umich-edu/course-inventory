'''
Migration to add MiVideo table foreign key
'''

from yoyo import step

__depends__ = {'0016.add_mivideo_creation_tables'}

steps = [
    step('''
        ALTER TABLE mivideo_media_courses 
          ADD CONSTRAINT fk_media_id 
          FOREIGN KEY (media_id)
          REFERENCES mivideo_media_created(id)
          ON UPDATE CASCADE
          ON DELETE CASCADE;
    '''),

]
