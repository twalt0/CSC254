#get_ipython().system('pip install psycopg2')

import psycopg2

command1 = ("""
        CREATE TABLE school_info (
            student_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            gpa_100 INTEGER
        )
        """)
insert_query = """
INSERT INTO school_info (student_id, name, gpa_100)
VALUES (%s, %s, %s)
"""

data_to_insert = [(70000001, 'Thomas Walter', 100),
                  (70000002, 'Vincent Gogh', 75),
                  (70000003, 'Salvador Dali', 80),
                  (70000004, 'Francisco Goya', 60)]

conn = None
try:
    conn = psycopg2.connect(host="localhost", port=5432, dbname="postgres", user="postgres", password="fakepasswordlolgottem")
    cur = conn.cursor()
    cur.execute(command1)
    for values in data_to_insert:
        cur.execute(insert_query, values)
    cur.close()
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()

conn.close()

