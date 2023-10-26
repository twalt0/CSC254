import psycopg2
from datetime import date
import requests
import json

"""Assignment 3:

Store that sells many items (One type of store)

Domain: Problem area we are to solve

Modeling: A representation of a real world thing from our Domain


STORE Domain) 
Data Points:

User
Items


purchases:
- id
- user id (serial)
- shopping list (array of item ids)

user:
- id (serial)
- name (text)
- membership (boolean)
-

items:
- id (serial)
- logo/label (image)
- name (text)
- price (number)
-

reviews:
- id (serial)
- user id (serial)
- item id (serial)
- rating (numeric)
- comment (text)
- 

"""

# ------------------------ POSTGRESQL API CLASS --------------------------------------


class PostgreSQL:
    def __init__(self, host, port, dbname, user, password):
        self.conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password)
        self.cur = self.conn.cursor()

    def create_table(self, tb_name, column_dict):
        self.reconnect()
        table_command = ""
        for name, value in column_dict.items():
            table_command += f"{name} {value}, "

        table_command = f"CREATE TABLE {tb_name}({table_command.rstrip(', ')})"
        try:
            self.cur.execute(table_command)
            self.conn.commit()
            return f"{tb_name} table created"
        except psycopg2.Error as e:
            self.conn.rollback()
            return f"An error occurred while creating {table_name}: {e}"

    def create_index(self, tb_name, col_name):
        index = f"CREATE INDEX idx_{tb_name}_{col_name} ON {tb_name} ({col_name})"
        self.cur.execute(index)
        self.conn.commit()
        return f"idx_{tb_name}_{col_name}"

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
        self.reconnect()
        delete_query = f"DROP TABLE IF EXISTS {tb_name}"
        try:
            self.cur.execute(delete_query)
            self.conn.commit()
            return f"{tb_name} table deleted"
        except psycopg2.errors.InFailedSqlTransaction:
            return f"{tb_name} table doesn't exist"

    def join_tables(self, tb1_name, tb2_name, on_column, join_type="", avg_col=None):
        if avg_col is not None:
            avg_select = f'AVG({avg_col}) AS avg_{avg_col}'

            join_query = f"""
                SELECT {tb1_name}.student_id, {tb1_name}.name, {avg_select}
                FROM {tb1_name}
                {join_type} JOIN {tb2_name} ON {tb1_name}.student_id = {tb2_name}.student_id
                GROUP BY {tb1_name}.student_id, {tb1_name}.name
            """

        else:
            join_query = f"SELECT * FROM {tb1_name} {join_type} JOIN {tb2_name} ON {tb1_name}.{on_column} = {tb2_name}.{on_column}"

        self.cur.execute(join_query)
        data = self.cur.fetchall()
        return data

    def create_enum(self, enum_type_name, enum_tuple):
        enum_tuple = str(enum_tuple)
        create_enum_query = f"""
        CREATE TYPE {enum_type_name} AS ENUM {enum_tuple};
        """
        try:
            self.cur.execute(create_enum_query)
            self.conn.commit()
            return f"Enumerable {enum_type_name} created"

        except psycopg2.errors.DuplicateObject:
            return f"Enumerable {enum_type_name} already made"

    def get_column_names(self, tb_name):
        # Helper method to get column names of a table
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tb_name}'"
        self.cur.execute(query)
        columns = self.cur.fetchall()
        return [column[0] for column in columns]

    def execute(self, full_query):
        self.cur.execute(full_query)
        self.conn.commit()
        return self.cur.fetchall()

    def reconnect(self):
        self.conn.reset()

    def close_connection(self):
        db_name = self.dbname
        self.cur.close()
        self.conn.close()
        return f"Connection to {db_name} closed."

# ---------------------------------------------------------------------------------------------------

# ----------------------- PFP IMAGE SCRAPE & BINARY CONVERSION --------------------------------------


image_url = "https://www.themoviedb.org/t/p/w500/lZpvHaRDSNqAEYUgaJed9Vxrx5p.jpg"

response = requests.get(image_url)

if response.status_code == 200:
    image_data = psycopg2.Binary(response.content)
else:
    image_data = psycopg2.Binary(b'\x12\x34\x56\x78\x9a\bcdef')

# ----------------------------------------------------------------------------------------------------

# --------------------------------- STORE DOMAIN -----------------------------------------------------

store_cols_dict = {
    "store_users": {
        "id": "SERIAL",
        "username": "VARCHAR(255)",
        "membership_date": "DATE",
        "profile_picture": "BYTEA"
    },
    "items": {
        "id": "SERIAL",
        "item_name": "VARCHAR(255)",
        "price": "NUMERIC(7,2)",
        "total_nutrition": "JSON",
        "serving_size_num": "INTEGER"
    },
    "transactions": {
        "id": "SERIAL",
        "user_id": "SERIAL",
        "transaction_date": "DATE",
        "payment_method": "PAYMENT"
    },
    "purchases": {
        "id": "SERIAL",
        "transaction_id": "SERIAL",
        "item_id": "SERIAL",
        "quantity": "INTEGER"
    }}

# ------------------------------------- STORE DATA ---------------------------------------------------

store_info_dict = {
    "store_users": {
        "id": [500000001, 500000002, 500000003, 500000004, 500000005, 500000006, 500000007],
        "username": ["Thomas Walter",
                     "Ronald McDonald",
                     "Mickey Mouse",
                     "Donald Duck",
                     "Bill Gates",
                     "Rick Roll",
                     "Lebron James"],
        "membership_date": [date(2023, 10, 1),
                            date(2023, 10, 5),
                            date(2023, 10, 10),
                            date(2023, 10, 10),
                            date(2023, 10, 12),
                            date(2023, 10, 15),
                            date(2023, 10, 20)],
        "profile_picture": [image_data, image_data, image_data, image_data, image_data, image_data, image_data]
    },
    "items": {
        "id": [300000001, 300000002, 300000003, 300000004, 300000005, 300000006, 300000007],
        "item_name": ["Sliced White Bread",
                      "Eggs (Dozen)",
                      "2% Milk (Gallon)",
                      "Fat Free Milk (Gallon)",
                      "Beefsteak Tomato",
                      "Whole Chicken Breast (4pc)",
                      "Oreos (8oz)"],
        "price": [3.50, 5.25, 4.99, 4.99, 0.79, 15.99, 5.99],
        "total_nutrition": [json.dumps({"calories": 420, "carbohydrates (g)": 75, "protein (g)": 15, "fat (g)": 9, "fiber (g)": 2}),
                            json.dumps({"calories": 720, "carbohydrates (g)": 2, "protein (g)": 60, "fat (g)": 56, "fiber (g)": 0}),
                            json.dumps({"calories": 240, "carbohydrates (g)": 12, "protein (g)": 16, "fat (g)": 12, "fiber (g)": 0}),
                            json.dumps({"calories": 400, "carbohydrates (g)": 40, "protein (g)": 10, "fat (g)": 24, "fiber (g)": 4}),
                            json.dumps({"calories": 160, "carbohydrates (g)": 30, "protein (g)": 12, "fat (g)": 0, "fiber (g)": 0}),
                            json.dumps({"calories": 720, "carbohydrates (g)": 0, "protein (g)": 160, "fat (g)": 4, "fiber (g)": 0}),
                            json.dumps({"calories": 980, "carbohydrates (g)": 90, "protein (g)": 4, "fat (g)": 16, "fiber (g)": 2})],
        "serving_size_num": [6, 6, 7, 7, 1, 4, 12]
    },
    "transactions": {
        "id": [700000001, 700000002, 700000003, 700000004],
        "user_id": [500000001, 500000003, 500000004, 500000007],
        "transaction_date": [date(2023, 10, 10),
                             date(2023, 10, 12),
                             date(2023, 10, 15),
                             date(2023, 10, 20)],
        "payment_method": ["Credit", "Credit", "Debit", "Cash"]
    },
    "purchases": {
        "id": [800000001, 800000002, 800000003, 800000004, 800000005,
               800000006, 800000007, 800000008, 800000009, 800000010],
        "transaction_id": [700000001, 700000001, 700000001, 700000002, 700000002,
                           700000002, 700000003, 700000003, 700000003, 700000004],
        "item_id": [300000001, 300000002, 300000004, 300000003, 300000005,
                    300000006, 300000003, 300000007, 300000002, 300000007],
        "quantity": [1, 1, 2, 3, 1, 2, 2, 1, 4, 2]
    }}

# ----------------------------------------------------------------------------------------------------------


# ------------------------- INITIALIZE POSTGRESQL CONNECTION -----------------------------------------------

postgres = PostgreSQL(host="localhost", port=5432, dbname="postgres", user="postgres", password="Womster*0808*")

# ----------------------------------------------------------------------------------------------------------


# ------------------------- CREATING AN ENUMERABLE ---------------------------------------------------------

postgres.create_enum("PAYMENT", ("Credit", "Debit", "Cash"))

# ----------------------------------------------------------------------------------------------------------


# ------------------------------------- TABLE CONSTRUCTION -------------------------------------------------

for table in store_cols_dict.keys():
    try:
        postgres.delete_table(table)
    except psycopg2.Error as e:
        print(f"An error occurred while deleting {table}: {e}")
        postgres.conn.rollback()

for table_name, table_cols in store_cols_dict.items():
    try:
        print(f"\nConstructing {table_name} table...")
        postgres.create_table(table_name, table_cols)
        print(f"\n{table_name} Constructed.")
        data = postgres.pull_data(table_name)
        print(f"There is no data in {table_name} yet. See here: {data}")
    except psycopg2.Error as e:
        print(f"An error occurred while creating {table_name}: {e}")
        postgres.conn.rollback()

# -----------------------------------------------------------------------------------------------------

# ------------------------------------- DATA POPULATION -----------------------------------------------

for table_name, table_data in store_info_dict.items():
    postgres.push_data(table_name, table_data)
    data = postgres.pull_data(table_name)
    print(f"\n\n{table_name} table has been populated. See below:\n{data}")


# ------------------------------------------------------------------------------------------------------

# ------------------------------------ CREATING INDEXES --------------------------------------------
for table_name, cols_dict in store_cols_dict.items():
    for col_name in cols_dict.keys():
        if "_id" in col_name:
            index = postgres.create_index(table_name, col_name)
            print(f"\n\n{table_name} has been indexed by {col_name}\nIndex Name: {index}")

# ---------------------------------------------------------------------------------------------------


"""
-- Table: public.purchases

-- DROP TABLE IF EXISTS public.purchases;

CREATE TABLE IF NOT EXISTS public.purchases
(
    id integer NOT NULL DEFAULT nextval('purchases_id_seq'::regclass),
    transaction_id integer NOT NULL DEFAULT nextval('purchases_transaction_id_seq'::regclass),
    item_id integer NOT NULL DEFAULT nextval('purchases_item_id_seq'::regclass),
    quantity integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.purchases
    OWNER to postgres;
-- Index: idx_purchases_item_id

-- DROP INDEX IF EXISTS public.idx_purchases_item_id;

CREATE INDEX IF NOT EXISTS idx_purchases_item_id
    ON public.purchases USING btree
    (item_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_purchases_transaction_id

-- DROP INDEX IF EXISTS public.idx_purchases_transaction_id;

CREATE INDEX IF NOT EXISTS idx_purchases_transaction_id
    ON public.purchases USING btree
    (transaction_id ASC NULLS LAST)
    TABLESPACE pg_default;
"""

