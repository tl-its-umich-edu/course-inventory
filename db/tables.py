tables = [
    {
        'name': 'course',
        'statement': '''
            CREATE TABLE IF NOT EXISTS course
            (   
                warehouse_id BIGINT NOT NULL UNIQUE,
                canvas_id INTEGER NOT NULL UNIQUE,
                name VARCHAR(100) NOT NULL UNIQUE,
                account_id INTEGER NOT NULL,
                created_at VARCHAR(25),
                workflow_state VARCHAR(25) NOT NULL,
                PRIMARY KEY (warehouse_id)
            )
            ENGINE=InnoDB
            CHARACTER SET utf8mb4
            COLLATE utf8mb4_0900_ai_ci;
        '''
    },
    {
        'name': 'user',
        'statement': '''
            CREATE TABLE IF NOT EXISTS user
            (
                warehouse_id BIGINT NOT NULL UNIQUE,
                canvas_id INTEGER NOT NULL UNIQUE,
                name VARCHAR(100) NOT NULL,
                uniqname VARCHAR(50) NOT NULL,
                workflow_state VARCHAR(25) NOT NULL,
                PRIMARY KEY (warehouse_id)
            )
            ENGINE=InnoDB
            CHARACTER SET utf8mb4
            COLLATE utf8mb4_0900_ai_ci;
        '''
    },
    {
        'name': 'enrollment',
        'statement': '''
            CREATE TABLE IF NOT EXISTS enrollment
            (
                warehouse_id BIGINT NOT NULL UNIQUE,
                canvas_id INTEGER NOT NULL,
                course_id BIGINT NOT NULL,
                course_section_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                workflow_state VARCHAR(25) NOT NULL,
                role_type VARCHAR(25) NOT NULL,
                PRIMARY KEY (warehouse_id),
                FOREIGN KEY (course_id) REFERENCES course(warehouse_id) ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (user_id) REFERENCES user(warehouse_id) ON DELETE CASCADE ON UPDATE CASCADE
            )
            ENGINE=InnoDB
            CHARACTER SET utf8mb4
            COLLATE utf8mb4_0900_ai_ci;
        '''
    },
]
