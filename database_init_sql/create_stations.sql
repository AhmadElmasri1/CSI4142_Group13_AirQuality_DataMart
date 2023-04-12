CREATE TABLE Stations (
	station_id SERIAL PRIMARY KEY,
	station_province VARCHAR(50),
	station_city VARCHAR(50),
	station_longitude NUMERIC(16,8),
	station_latitude NUMERIC(16,8),
	station_address VARCHAR(200)
);