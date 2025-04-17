import psycopg2
import os
dbname = "piscineds"
user = "blarger"
password = "mysecretpassword"
host = "localhost"
port = "5432"

try:
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(cur_dir, "fusion.sql")
    with open(sql_file_path, "r") as sql_file:
        sql_script = sql_file.read()
    print("SQL code has been imported!")
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    print("Connected to postgres!")
    cursor = conn.cursor()
    cursor.execute(sql_script)
    print("SQL script executed successfully!")
    print("Data has been fetched from the table.")
    conn.commit()
except Exception as e:
    print(f"Error: {str(e)}")

finally:
    cursor.close()
    conn.close()