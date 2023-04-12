import psycopg2
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Set up a connection to the database
conn = psycopg2.connect(
    host="localhost",   
    database="airqualitydatamart",
    user="postgres",
    password="272121"
)

df = pd.read_csv('PM25_Total_Data_test.csv',low_memory=False)

#NAPSID,Station_Name,Location_Address,Postal_Code,Latitude,Longitude,City,P/T,Status
station_df = pd.read_csv('TotalData\StationData.csv', low_memory=False, encoding="ISO-8859-1")
#df = pd.read_csv('AirQuality_SpreadSheets\HourlyData_ByYear\Hourly_2004\SO2_2004.csv',low_memory=False,skiprows=5, encoding="ISO-8859-1")


chemical_table = "chemical_type"
station_table = "stations"
fact_table = "measurements"
time_table = "time"

cur = conn.cursor()

chemical_columns = ["chemical_name", "measurement_unit"]
station_columns = ["station_province", "station_city", "station_latitude", "station_longitude", "station_address"]
fact_columns = ["time_id", "chemical_id", "station_id", "value"]
time_columns = ["minute", "hour", "day", "month", "year"]


def insert_data(start_index, end_index, dataframe):
    dataframe_holder = dataframe.copy(deep=True)

    conn = psycopg2.connect(
        host="localhost",   
        database="airqualitydatamart",
        user="postgres",
        password="272121"
    )

    cur = conn.cursor()

    for index, row in dataframe_holder.iloc[start_index:end_index].iterrows():
        date = str(row["Date"])
        year = int(date[:4])
        month = int(date[4:6])
        day = int(date[6:])

        chemical = row["Pollutant"]
        unit = row["Unit"]
        cur.execute(f"SELECT chemical_id FROM {chemical_table} WHERE chemical_name=%s", (chemical,))
        result = cur.fetchone()
        if result is None:
            cur.execute(f"INSERT INTO {chemical_table} (chemical_name, measurement_unit) VALUES (%s, %s) RETURNING chemical_id", (chemical, unit))
            chemical_id = cur.fetchone()[0]
        else:
            chemical_id = result[0]
            conn.commit()

        station_information = station_df[station_df['NAPSID'] == row['NAPSID']].copy(deep=True)
        #station_information = station_information.reindex(index = [0])


        province = row["P/T"]
        city = row["City"]
        latitude = round(row["Latitude"],8)
        longitude = round(row["Longitude"],8)
        if(station_information is None):
            address = station_information["Longitude"]  + " " + station_information["Latitude"]
            print("\n\n yeet \n\n")
        else:
            address = station_information["Location_Address"]  + " " + station_information["Postal_Code"]
        cur.execute(f"SELECT station_id FROM {station_table} WHERE station_province=%s AND station_city=%s AND station_latitude=%s AND station_longitude=%s", (province, city, latitude, longitude))
        result = cur.fetchone()
        if result is None:
            cur.execute(f"INSERT INTO {station_table} (station_province, station_city, station_latitude, station_longitude, station_address) VALUES (%s, %s, %s, %s, %s) RETURNING station_id", (province, city, latitude, longitude, address.iloc[0]))
            station_id = cur.fetchone()[0]
            conn.commit()
        else:
            station_id = result[0]  
   
        cur.execute(f"INSERT INTO {fact_table} (time_id, chemical_id, station_id, value) VALUES (%s, %s, %s, %s)", (time_id, chemical_id, station_id, value))

        hour_columns = [f"H{i:02d}" for i in range(1, 25)]
        for i, hour in enumerate(hour_columns):
            value = row[hour]
        
            # Check if the time already exists
            cur.execute(f"SELECT time_id FROM {time_table} WHERE day=%s AND month=%s AND year=%s AND hour=%s", (day, month, year, i+1))
            result = cur.fetchone()
            if result is None:
                cur.execute(f"INSERT INTO {time_table} (minute, hour, day, month, year) VALUES (0, %s, %s, %s, %s) RETURNING time_id", (i+1, day, month, year))
                time_id = cur.fetchone()[0]
            else:
                time_id = result[0]

        if(index % 500 == 0):
            holder = str(index) + ': ' + str(station_id) + ', ' + str(longitude) + ',' + str(latitude) + ',' + str(address.iloc[0])
            print(holder)
            print(address)
            print(address.iloc[0])
            conn.commit()

        conn.commit()
        cur.close()

subdivisions = 9
len_iteration = df.shape[0]/subdivisions
with ThreadPoolExecutor(9) as executor:
    for div in range(0,(subdivisions-1)):
        start_index = int(div * len_iteration)
        end_index = int((div+1) * len_iteration)-1
        indices = str(start_index) + "," + str(end_index)
        print(indices)
        executor.submit(insert_data, start_index, end_index, df)
        



#conn.commit()
#cur.close()
conn.close()