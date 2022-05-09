CREATE DATABASE IF NOT EXISTS citibyke_trip COMMENT 'CitiByke trip data';


CREATE USER IF NOT EXISTS citibyke
NOT IDENTIFIED
HOST ANY
DEFAULT DATABASE citibyke_trip
;

grant ALL on *.* to citibyke;
