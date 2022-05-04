create table citibyke_trip.stg_citibike_tripdata (
	ride_id				    String default '-1',
	rideable_type		    String default '-1',
	tripduration			String default '-1',
	starttime				datetime,
	stoptime				datetime,
	start_station_id		String,
	start_station_name		String,
	start_lat				String,
	start_lng				String,
	end_station_id			String,
	end_station_name		String,
	end_lat					String,
	end_lng					String,
	member_casual		    String default '-1',
	bikeid					String default '-1',
	usertype				String default '-1',
	birth_year				String default '-1',
	gender					String default '0',
	city                	String,
	load_dttm           	datetime default now()
)
ENGINE=MergeTree()
ORDER BY (starttime, start_station_id, end_station_id)
;