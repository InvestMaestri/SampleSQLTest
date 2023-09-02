import os
import re
import random
import mysql.connector

import numpy as np
import matplotlib.pyplot as plt


# Utility Functions ------------------
def read_file(file_path):
    with open(file_path, 'r') as file:
        names = [line.strip() for line in file]
    return names


class RandPersonGenerator:
    def __init__(self):
        self.first_name_list = self.read_file('first_names.txt')
        self.last_name_list = self.read_file('last_names.txt')
        self.address_list = self.read_file('addresses.txt')
        self.phone_prefix_data = [('AL', '205, 251, 256, 334, 659, 938'),
                                  # ... (remaining phone prefix data)
                                 ]

    def read_file(self, filename):
        with open(filename, 'r') as file:
            return [line.strip() for line in file]

    def generate_random_person(self):
        first_name = random.choice(self.first_name_list)
        last_name = random.choice(self.last_name_list)
        address = random.choice(self.address_list)
        notice_id = random.randint(1000, 9999)
        if random.randint(1, 100) <= 10:
            notice_id = 0

        email_1 = f'{first_name}{last_name}@gmail.com'
        if random.randint(1, 100) <= 5:
            email_pay = f'{random.choice(self.first_name_list)}@ymail.com'
        else:
            email_pay = email_1

        state_pattern = r'\b([A-Z]{2})\b'
        match = re.search(state_pattern, address)
        if match:
            state_id = match.group(1)
        state_prefix = '123'  # Convert to string
        for state, prefixes in self.phone_prefix_data:
            if state == state_id:
                prefix_list = prefixes.split(', ')
                state_prefix = random.choice(prefix_list)
                break

        phone = state_prefix + str(random.randint(1000000, 9999999))
        if random.randint(1, 100) <= 5:
            pay_phone = random.randint(1000000000, 9999999999)
        else:
            pay_phone = phone

        return last_name, first_name, address, notice_id, email_1, email_pay, phone, pay_phone


def get_number(string):
    return list(map(int, (re.findall(r'\d+', str(string)))))


# MySQL ---------------------------------------------
# Login
in_pass = input(f'Input Password:\n')

os.environ['host'] = '192.168.1.15'
os.environ['user'] = 'admin'
os.environ['password'] = in_pass
os.environ['database'] = 'test_database'
os.environ['table'] = 'table_1'

# Get environment variables
host = os.getenv('host')
user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database')
table = os.getenv('table')

# Connect
connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
cursor = connection.cursor()

in_pass = random.randint(1000000, 9999999)

# Check for accessible databases
cursor.execute("SHOW DATABASES")
for db in cursor:
    print(db)

# Create a blank test database "test_angeion"
create_database = "CREATE DATABASE test_angeion"
cursor.execute(create_database)
connection.commit()

# Create table "table_1"
create_table = """CREATE TABLE table_1 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    last_name VARCHAR(255),
    first_name VARCHAR(255),
    address VARCHAR(255),
    notice_id INT,
    email VARCHAR(255),
    pay_email VARCHAR(255),
    phone VARCHAR(10),
    pay_phone VARCHAR(10)
     )"""
cursor.execute(create_table)
connection.commit()

# Check table created. No duplicates
'''check_query = "SHOW TABLES"
cursor.execute(check_query)
for table in cursor:
    print(table)'''

# Populate 1000 rows
person_generator = RandPersonGenerator()
insert_query = f"""INSERT INTO {table} (last_name, first_name, address, notice_id, email, pay_email, phone, pay_phone)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
index = 0
while index <= 9999:
    person_data = person_generator.generate_random_person()  # Use the method to get random person data
    cursor.execute(insert_query, person_data)  # Insert the person data
    index += 1

connection.commit()

# Check all rows for completeness
all_row_query = f"""SELECT *
                FROM {table}"""
cursor.execute(all_row_query)
rows = cursor.fetchall()
for row in rows:
    print(row)

# If limited read
all_row_query = f"""SELECT *
                FROM {table}
                LIMIT 300"""
cursor.execute(all_row_query)
rows = cursor.fetchall()
for row in rows:
    print(row)

# If very limited read
count_row_query = f"""SELECT COUNT(*)
                      FROM {table}"""
cursor.execute(count_row_query)
row_count = cursor.fetchall()
print(row_count)

# --------------------------------------------------------------------------
# Fraud Checking:

# Possible Fraud Flags <- Is Pay_Email != Email, Is Pay_Phone != Phone, Is Pay_Phone out of state, Is Phone Valid?
# (I.e., 202 '555' 8899), Is Notice_ID = 0?

# We cannot duplicate table (SPACE CONSTRAINT), we cannot load to local memory (DATA SAFETY, HARDWARE CONSTRAINT),
# and we cannot change the original table (DATA SAFETY).

# Run everything in queries!

# Email Mismatch COUNT(*) count all instead of retrieve all to save on execution costs
fraud_query_email = (f"""SELECT COUNT(*)
                         FROM {table} 
                         WHERE email <> pay_email""")
cursor.execute(fraud_query_email)
email_mismatch = cursor.fetchall()
print(f'Email Mismatches: {email_mismatch}')

# Phone Mismatch ----------------------------
fraud_query_phone = (f"""SELECT COUNT(*)
                         FROM {table} 
                         WHERE phone <> pay_phone""")
cursor.execute(fraud_query_phone)
phone_mismatch = cursor.fetchall()
print(f'Phone Mismatches: {phone_mismatch}')

# Invalid Phone Number ----------------------------
query_invalid_phone = (f"""SELECT COUNT(*)
                         FROM {table} 
                         WHERE SUBSTRING(phone, 345, 3) = '555' OR SUBSTRING(pay_phone, 345, 3) = '555' """)
cursor.execute(query_invalid_phone)
phone_invalid = cursor.fetchall()
print(f'Invalid Phones: {phone_invalid}')

# Out of state phone ----------------------------
out_of_state_query_1 = f"""
SELECT t1.*, sc.codes AS state_codes
FROM table_1 AS t1
LEFT JOIN state_codes AS sc ON SUBSTRING_INDEX(RIGHT(t1.address, 8), ' ', 1) = sc.state_id
WHERE t1.pay_phone IS NOT NULL
  AND NOT FIND_IN_SET(SUBSTRING(t1.pay_phone, 1, 3), REPLACE(sc.codes, ' ', ''));
"""

cursor.execute(out_of_state_query_1)
oos_rows = cursor.fetchall()
oos_count = len(oos_rows)
print(f'Out Of State Numbers count: {oos_count}')

# Missing claim ID ----------------------------
fraud_missing_id = (f"""SELECT COUNT(*)
                    FROM {table}
                    WHERE notice_id = 0""")
cursor.execute(fraud_missing_id)
missing_id = cursor.fetchall()
print(f'Missing IDs: {missing_id}')

# Highly flagged count (IF UNSURE) since we know it is between 0 and 56
high_flags_count = (f"""SELECT COUNT(*)
                  FROM {table}
                  WHERE email <> pay_email AND phone <> pay_phone""")
cursor.execute(high_flags_count)
high_flagged_count = cursor.fetchall()
print(f'High flagged: {high_flagged_count}')

# Highly flagged ----------------------------
high_flags = (f"""SELECT *
                  FROM {table}
                  WHERE email <> pay_email AND phone <> pay_phone""")
cursor.execute(high_flags)
high_flagged = cursor.fetchall()
print(f'\nHigh flagged entries:')
high_flag_count = 0
for i in high_flagged:
    high_flag_count += 1
    print(i)

# Done <- Close Connections -------------
cursor.close()
connection.close()

num_miss_id = get_number(missing_id)
num_oos = get_number(oos_count)
num_phone_inv = get_number(phone_invalid)
num_phone_mis = get_number(phone_mismatch)
num_email_mis = get_number(email_mismatch)
num_high_flags = get_number(high_flagged_count)
total_rows = row_count[0]
total_rows = int(total_rows[0])

flag_count = sum(num_miss_id + num_oos + num_phone_inv + num_phone_mis + num_email_mis + num_high_flags)
flag_pct = flag_count/total_rows
print(f'\nTotal Rows: {total_rows}')
print(f'Flag Count: {flag_count}')
print(f'Flag Percentage: {flag_pct}%')

arr = np.array([num_miss_id, num_oos, num_phone_inv, num_phone_mis, num_email_mis, num_high_flags])
flat_arr = arr.flatten()
labels = ["Missing Claim ID", "Pay Number from Out of State", "Invalid Phone Number", "Pay Number Mismatch",
          "Pay Email Mismatch", "High Flag Count"]

total_count = np.sum(flat_arr)
percentages = (flat_arr / total_count) * 100
plt.pie(flat_arr, labels=labels, autopct=lambda p: '{:.2f}%'.format(p))
plt.title("Pie Chart of Flags")
plt.show()
