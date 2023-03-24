CREATE TABLE Measurements (
	station_id SERIAL PRIMARY KEY,
	time_id INTEGER REFERENCES time(time_id),
	chemical_id INTEGER REFERENCES chemical_type(chemical_id),
	station_id INTEGER REFERENCES stations(station_id),
	value FLOAT(10, 6)
);