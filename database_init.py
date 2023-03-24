import psycopg2

# connect to the server
conn = psycopg2.connect(
    host="localhost",
    # database="AirQualityDataMart",
    user="postgres",
    password="password"
)

# Set autocommit to True to avoid running in a transaction block
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE DATABASE airqualitydatamart;")
cur.close()
conn.close()



conn = psycopg2.connect(
    host="localhost",
    database="airqualitydatamart",
    user="postgres",
    password="password"
)
cur = conn.cursor()
# read sql files
with open('database_init_sql/create_time.sql', 'r') as file:
    sql = file.read()
    print(sql)
    cur.execute(sql)

with open('database_init_sql/create_chemical_type.sql', 'r') as file:
    sql = file.read()
    print(sql)
    cur.execute(sql)

with open('database_init_sql/create_stations.sql', 'r') as file:
    sql = file.read()
    print(sql)
    cur.execute(sql)

with open('database_init_sql/create_fact_table.sql', 'r') as file:
    sql = file.read()
    print(sql)
    cur.execute(sql)

    
conn.commit()
cur.close()
conn.close()