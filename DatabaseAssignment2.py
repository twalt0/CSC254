import psycopg2

class PostgreSQL:
    def __init__(self, host, port, dbname, user, password):
        self.conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password)
        self.cur = self.conn.cursor()

    def create_table(self, tb_name, column_dict):
        table_command = ""
        for name, value in column_dict.items():
            table_command += f"{name} {value}, "

        table_command = f"CREATE TABLE {tb_name}({table_command.rstrip(', ')})"
        self.cur.execute(table_command)
        self.conn.commit()
        return f"{tb_name} table created"

    def pull_data(self, tb_name):
        query = f"SELECT * FROM {tb_name}"
        self.cur.execute(query)
        data = self.cur.fetchall()
        return data

    def push_data(self, tb_name, value_dict):
        insert_keys = ', '.join(value_dict.keys())
        insert_values = ', '.join(['%s'] * len(value_dict))

        insert = f"INSERT INTO {tb_name} ({insert_keys}) VALUES ({insert_values})"
        for values in zip(*value_dict.values()):
            self.cur.execute(insert, values)

        self.conn.commit()
        return "Table Populated"
    
    def delete_table(self, tb_name):
        delete_query = f"DROP TABLE IF EXISTS {tb_name}"
        self.cur.execute(delete_query)
        self.conn.commit()
        return f"{tb_name} table deleted"

    def join_tables(self, tb1_name, tb2_name, on_column, avg=False):
        if avg:
            avg_select = 'AVG(grade) AS avg_grade'
        
            join_query = f"""
                SELECT {students_table}.student_id, {students_table}.name, {avg_select}
                FROM {students_table}
                INNER JOIN {classes_table} ON {students_table}.student_id = {classes_table}.student_id
                GROUP BY {students_table}.student_id, {students_table}.name
            """
        
        else:
            join_query = f"SELECT * FROM {tb1_name} LEFT INNER JOIN {tb2_name} ON {tb1_name}.{on_column} = {tb2_name}.{on_column}"

        self.cur.execute(join_query)
        data = self.cur.fetchall()
        return data

    def get_column_names(self, table_name):
        # Helper method to get column names of a table
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        self.cur.execute(query)
        columns = self.cur.fetchall()
        return [column[0] for column in columns]

    def close_connection(self):
        self.cur.close()
        self.conn.close()

classes_table = "classes"
students_table = "students"

classes_cols_dict = {'student_id': 'SERIAL',
            'class': 'VARCHAR(255)',
            'grade': 'INTEGER'}

students_cols_dict = {'student_id': 'SERIAL', 
                      'name': 'VARCHAR(255)'}

classes_info_dict = {'student_id': [700000001, 700000002, 700000003, 700000001, 700000002, 700000003, 700000001, 700000002, 700000003], 
                     "class": ['csc220', 'csc220', 'csc220', 'csc254', 'csc254', 'csc254', 'csc291', 'csc291', 'csc291'], 
                     "grade": [80, 80, 95, 60, 75, 65, 85, 100, 80]}

students_info_dict = {'student_id': [700000001, 700000002, 700000003], 
                     "name": ["Thomas Walter", "John Adams", "Abraham Lincoln"]}


postgres = PostgreSQL(host="localhost", port=5432, dbname="postgres", user="postgres", password="")
postgres.delete_table(classes_table)
postgres.delete_table(students_table)


## First we create a table of student ids and their names, then check to see that the data is posted

print("Constructing students table...")
postgres = PostgreSQL(host="localhost", port=5432, dbname="postgres", user="postgres", password="")
postgres.create_table(students_table, students_cols_dict)
print("\nReading data from students table before adding students:")
data = postgres.pull_data(students_table)
print(data)
print("\nReading data from students table after adding students:")
postgres.push_data(students_table, students_info_dict)
data = postgres.pull_data(students_table)
print(data)


## Next we create a table of student ids and corresponding class grades, then we check to see data is posted

print("\n\nConstructing classes table...")
postgres.create_table(classes_table, classes_cols_dict)
print("\nReading data from classes table before adding classes")
data = postgres.pull_data(classes_table)
print(data)
print("\nReading data from classes table after adding classes:")
postgres.push_data(classes_table, classes_info_dict)
data = postgres.pull_data(classes_table)
print(data)


## Lastly we are joining the tables, pulling the average of the classes table
print(f"\n\nReading data from both tables after joining and obtaining averages:")
data = postgres.join_tables(students_table, classes_table, "student_id", True)
print(f"{data}\n")
postgres.close_connection()


print("\nReturning each students name and gpa:")
for datum in data:
    print(f"Name: {datum[1]} (GPA [100 scale]: {'{:.2f}'.format(datum[2])}")

