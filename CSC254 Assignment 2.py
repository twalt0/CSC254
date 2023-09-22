#get_ipython().system('pip install psycopg2')

import psycopg2

table_commands = ("""
        CREATE TABLE students (
            student_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            fafsa BOOLEAN
        )
        ""","""
        CREATE TABLE grades (
            student_id SERIAL PRIMARY KEY,
            gpa_100 INTEGER,
            csc254 INTEGER
        )
        """)

insert_query = ["""
INSERT INTO students (student_id, name, fafsa)
VALUES (%s, %s, %s)
""","""
INSERT INTO grades (student_id, gpa_100, csc254)
VALUES (%s, %s, %s)
"""]

students_to_insert = [(70000001, 'Thomas Walter', False),
                  (70000002, 'Vincent Gogh', True),
                  (70000003, 'Salvador Dali', False),
                  (70000004, 'Francisco Goya', True)]

grades_to_insert = [(70000001, 100, 100),
                  (70000002, 75, 85),
                  (70000003, 80, 80),
                  (70000004, 60, 50)]

conn = None
try:
    conn = psycopg2.connect(host="localhost", port=5432, dbname="postgres", user="postgres", password="Womster*0808*")
    cur = conn.cursor()
        
    for values in table_commands:
        cur.execute(values)
            
    for values in students_to_insert:
        cur.execute(insert_query[0], values)
        
    for values in grades_to_insert:
        cur.execute(insert_query[1], values)
        
    cur.close()
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()

conn.close()

