import numpy as np
import datetime
import time


import psycopg2
import requests
import json
import random

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
            return f"An error occurred while creating {tb_name}: {e}"

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

    def value_array(self, tb_name, col_name):
        self.reconnect()
        query = f"SELECT {col_name} FROM {tb_name}"
        self.cur.execute(query)
        array = self.cur.fetchall()
        return list(np.array(array)[:,0])

    def max_value(self, tb_name, col_name):
        self.reconnect()
        query = f"SELECT {col_name} FROM {tb_name} ORDER BY {col_name} DESC LIMIT 1"
        self.cur.execute(query)
        value = self.cur.fetchall()[0][0]
        return value

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
        except psycopg2.Error:
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

        except psycopg2.Error:
            return f"Enumerable {enum_type_name} already made"

    def get_column_names(self, tb_name):
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
        self.cur.close()
        self.conn.close()
        return f"Connection closed."


image_url = "https://www.themoviedb.org/t/p/w500/lZpvHaRDSNqAEYUgaJed9Vxrx5p.jpg"

response = requests.get(image_url)

if response.status_code == 200:
    image_data = psycopg2.Binary(response.content)
else:
    image_data = psycopg2.Binary(b'\x12\x34\x56\x78\x9a\bcdef')


"""
We want to simulate a small sample of a store's data influx management.

To represent this, I am going to create a loop using random selection to grab items, 
"""

# ------------------------------ Initializing Connection & Development Constants -------------------------------------
postgres = PostgreSQL(host="localhost", port=5432, dbname="postgres", user="postgres", password="Womster*0808*")
#const_time = datetime.datetime.now()
#end_time = const_time + datetime.timedelta(minutes=1)
pay_method = ["Credit", "Debit", "Cash"]

# --------------------------------------------------------------------------------------------------------------------

# ------------------------------------ Start Loop to Create Data ------------------------------------------------------
while True:
    # ------------------------------------ Local Loop Constants -----------------------------------------------------
    user_ids = postgres.value_array("store_users", "id")
    item_ids = postgres.value_array("items", "id")
    transaction_id = postgres.max_value("transactions", "id") + 1
    random_user = random.choice(user_ids)
    purchase_id = postgres.max_value("purchases", "id") + 1
    order = {"purchases": {
        "id": [],
        "transaction_id": [],
        "item_id": [],
        "quantity": []
    },
    "transactions": {
        "id": [],
        "user_id": [],
        "transaction_date": [],
        "payment_method": []
    }}
    prev_item_ids = []
    # ----------------------- Selecting Item and Evaluating if it was added to Order Already -------------------------
    for i in range(random.randint(1, 3)):
        random_item = random.choice(item_ids)
        while random_item in prev_item_ids:
            random_item = random.choice(item_ids)
        prev_item_ids.append(random_item)
        random_quantity = random.randint(1, 3)
        # ------------------------- Adding Items for Purchase to Order ----------------------------------------------
        print(f"User ({random_user}) is purchasing {random_quantity} of Item ({random_item})")
        order["purchases"]["id"].append(purchase_id)
        order["purchases"]["transaction_id"].append(transaction_id)
        order["purchases"]["item_id"].append(random_item)
        order["purchases"]["quantity"].append(random_quantity)
        purchase_id = purchase_id + 1

    # ------------------------------------ Adding Transaction Details to Order --------------------------------------
    order["transactions"]["id"].append(transaction_id)
    order["transactions"]["user_id"].append(random_user)
    order["transactions"]["transaction_date"].append(datetime.date.today())
    order["transactions"]["payment_method"].append(random.choice(pay_method))

    # ------------------------------ Creating Random User P(1/N) times ----------------------------------------------
    # N ----> Number of Users in DataBase
    if random_user == random.choice(user_ids):
        print(f"Random User Being Generated.")
        user_dic = {
            "id": [],
            "username": [],
            "membership_date": [],
            "profile_picture": []
        }
        # --------------------------- Adding User Details to Order -------------------------------------------------
        first_names = ["Thomas", "Micah", "Josh", "Joan", "Cat", "Bill", "Ted"]
        last_names = ["Walter", "Weatherly", "Aperture", "Bishop", "Hope"]
        gen_name = random.choice(first_names) + " " + random.choice(last_names)
        user_dic["id"].append(postgres.max_value("store_users", "id") + 1)
        user_dic["username"].append(gen_name)
        user_dic["membership_date"].append(datetime.date.today())
        user_dic["profile_picture"].append(image_data)
        order["store_users"] = user_dic
        print(f"Random User {user_dic['username']} Created.")

    # --------------------- Adding Purchase, Transaction, (and User if selected) Data to Database ------------------
    for table_name, table_data in order.items():
        postgres.push_data(table_name, table_data)
        data = postgres.pull_data(table_name)
        print(f"\n\n{table_name} table has been populated with {table_data}")

    # -------------------------------- Simulate Time Between Transactions ------------------------------------------
    time.sleep(5)  # Wait number of seconds to simulate lull between transactions
    # const_time = datetime.datetime.now()   # Re-examine time in Development

