import pymysql
import sys
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = db_connection = pymysql.connect(
        host = os.getenv('DB_HOST'),
        port = 3308,
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
        db = os.getenv('DB_NAME'),
        charset = 'utf8mb4'
    )
except pymysql.Error as e:
    print("Error connecting to MariaDB")
    sys.exit(1)

cur = conn.cursor()

#query = "select * from jsol_attendance_record"
#cur.execute(query)

#result = cur.fetchall()

#print(type(result))
#((2, 3, 1, datetime.datetime(2024, 10, 4, 23, 8, 1), None, None),)

df = pd.read_csv('data/la_ll.csv', na_filter=False)
df = df.replace({np.nan: None, 'NULL': None})
csv_columns = df.columns.tolist()

for _, row in df.iterrows():
    insert_query = f"INSERT INTO jsol_attendance_record ({', '.join(csv_columns)}) VALUES ({', '.join(['%s']*len(csv_columns))})"
    cur.execute(insert_query, tuple(row))

conn.commit()
cur.close()
conn.close()