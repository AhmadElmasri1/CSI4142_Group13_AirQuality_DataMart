import psycopg2
import pandas as pd

# Set up a connection to the database
conn = psycopg2.connect(
    host="localhost",
    database="airqualitydatamart",
    user="postgres",
    password="password"
)

df = pd.read_csv('CO_Total_Data_Test.csv',low_memory=False)

chemical_table = "chemical_type"
station_table = "stations"
fact_table = "measurements"
time_table = "time"

cur = conn.cursor()

chemical_columns = ["chemical_name", "measurement_unit"]
station_columns = ["station_province", "station_city", "station_latitude", "station_longitude"]
fact_columns = ["time_id", "chemical_id", "station_id", "value"]
time_columns = ["second", "hour", "day", "month", "year"]

for index, row in df.iterrows():
    date = str(row["Date"])
    year = int(date[:4])
    month = int(date[4:6])
    day = int(date[6:])
    
    hour_columns = [f"H{i:02d}" for i in range(1, 25)]
    for i, hour in enumerate(hour_columns):
        value = row[hour]
        
        # Check if the time already exists
        cur.execute(f"SELECT time_id FROM {time_table} WHERE day=%s AND month=%s AND year=%s AND hour=%s", (day, month, year, i+1))
        result = cur.fetchone()
        if result is None:
            cur.execute(f"INSERT INTO {time_table} (second, hour, day, month, year) VALUES (0, %s, %s, %s, %s) RETURNING time_id", (i+1, day, month, year))
            time_id = cur.fetchone()[0]
        else:
            time_id = result[0]

    
    chemical = row["Pollutant"]
    unit = row["Unit"]
    cur.execute(f"SELECT chemical_id FROM {chemical_table} WHERE chemical_name=%s", (chemical,))
    result = cur.fetchone()
    if result is None:
        cur.execute(f"INSERT INTO {chemical_table} (chemical_name, measurement_unit) VALUES (%s, %s) RETURNING chemical_id", (chemical, unit))
        chemical_id = cur.fetchone()[0]
    else:
        chemical_id = result[0]


    province = row["P/T"]
    city = row["City"]
    latitude = row["Latitude"]
    longitude = row["Longitude"]
    cur.execute(f"SELECT station_id FROM {station_table} WHERE station_province=%s AND station_city=%s", (province, city))
    result = cur.fetchone()
    if result is None:
        cur.execute(f"INSERT INTO {station_table} (station_province, station_city, station_latitude, station_longitude) VALUES (%s, %s, %s, %s) RETURNING station_id", (province, city, latitude, longitude))
        station_id = cur.fetchone()[0]
    else:
        station_id = result[0]    
    print(index)

    cur.execute(f"INSERT INTO {fact_table} (time_id, chemical_id, station_id, value) VALUES (%s, %s, %s, %s)", (time_id, chemical_id, station_id, value))

conn.commit()
cur.close()
conn.close()