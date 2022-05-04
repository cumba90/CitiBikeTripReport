create table citibyke_trip.dm_tripdata_avg (
	report_date				date,
	avg_trip		        UInt32,
	load_dttm           	datetime default now()
)
ENGINE=MergeTree()
PRIMARY KEY (report_date)
;