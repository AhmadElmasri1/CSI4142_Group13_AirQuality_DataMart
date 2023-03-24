CREATE TABLE Stations (
	station_id SERIAL PRIMARY KEY,
	station_province VARCHAR(50),
	station_city VARCHAR(50),
	station_longitude NUMERIC(10,6),
	station_latitude NUMERIC(10,6)
);