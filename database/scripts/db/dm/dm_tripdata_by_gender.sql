create table citibyke_trip.dm_tripdata_by_gender (
	report_date				date,
	gender                  UInt32,
	trip_count		        UInt32,
	load_dttm           	datetime default now()
)
ENGINE=MergeTree()
PRIMARY KEY (report_date, gender)
;