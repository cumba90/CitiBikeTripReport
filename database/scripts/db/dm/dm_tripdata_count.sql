create table citibyke_trip.dm_tripdata_count (
	report_date				date,
	trip_count		        UInt32,
	load_dttm           	datetime default now()
)
ENGINE=MergeTree()
PRIMARY KEY (report_date)
;