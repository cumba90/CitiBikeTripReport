import os
import boto3
import zipfile
import pandas as pd
from clickhouse_driver import Client
from dag_config import common_settings
import logging as log

ch_client = Client(common_settings['ch_host'],
                   port=common_settings['ch_port'],
                   user=common_settings['ch_user'],
                   database=common_settings['ch_database'], settings={'use_numpy': True})

def stg_load_tripdata():


    s3 = boto3.resource(service_name="s3",
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id=common_settings['aws_access_key_id'],
                        aws_secret_access_key=common_settings['aws_secret_access_key']
                        )

    bucket = s3.Bucket("in-citibike-tripdata")

    ch_client.execute("TRUNCATE TABLE citibyke_trip.stg_citibike_tripdata")

    processing_files_path = common_settings["processing_files_path"]

    f = open(processing_files_path, 'r')
    log.info("Reading content of {} ...".format(processing_files_path))
    for obj in f:
        zip_file = obj.split(";")[0].strip()
        file_dest_path = common_settings["file_landing_path"]
        path_to_zip_file = file_dest_path + zip_file

        log.info("Downloading of {} has started. It will saved in {}".format(zip_file, file_dest_path))
        bucket.download_file(zip_file, path_to_zip_file)
        log.info("Downloading of {} has ended".format(zip_file))

        log.info("Finding csv files in archive...")
        with zipfile.ZipFile(path_to_zip_file) as zf:
            for file in zf.namelist():
                if file.endswith(".csv") and '/' not in file:
                    log.info("Extracting {}".format(file))
                    zf.extract(file, file_dest_path)
                    if file[0:2] == "JC":
                        city = "JC"
                    else:
                        city = "NY"

                    log.info("Reading {} into PD dataframe".format(file))
                    df = pd.read_csv(file_dest_path + file)
                    df = df.assign(city=city)
                    log.info(list(df.columns))
                    log.info(file_dest_path + file + ", " + str(len(df.index)))

                    log.info("Start INSERTING....")
                    try:
                        if 'ride_id' in list(df.columns):
                            df.columns = ["ride_id", "rideable_type", "starttime", "stoptime",
                                          "start_station_name", "start_station_id", "end_station_name", "end_station_id",
                                          "start_lat", "start_lng", "end_lat", "end_lng", "member_casual", "city"]

                            df = df.astype(str)
                            log.info(df.info())
                            log.info(df)

                            ch_client.insert_dataframe(
                                """INSERT INTO citibyke_trip.stg_citibike_tripdata(ride_id,rideable_type,starttime,stoptime,
                                start_station_name,start_station_id,end_station_name,end_station_id,start_lat,
                                start_lng,end_lat,end_lng,member_casual,city)
                                VALUES""", df
                            )
                        else:
                            df.columns = ["tripduration", "starttime", "stoptime", "start_station_id",
                                "start_station_name", "start_lat", "start_lng", "end_station_id", "end_station_name",
                                "end_lat", "end_lng", "bikeid", "usertype", "birth_year", "gender", "city"]

                            df = df.astype(str)
                            log.info(df.info())
                            log.info(df)

                            ch_client.insert_dataframe(
                                """INSERT INTO citibyke_trip.stg_citibike_tripdata(tripduration,starttime,stoptime,start_station_id,
                                start_station_name,start_lat,start_lng,end_station_id,end_station_name,end_lat,end_lng,
                                bikeid,usertype,birth_year,gender,city)
                                VALUES""", df
                            )
                        log.info("Inserting ENDED")
                    except:
                        log.error("INSERTING ERROR !!!!!!!!")
                        raise
                    os.remove(file_dest_path + file)

        os.remove(path_to_zip_file)

def dm_load_tripdata_cout():

    log.info("Clean up data before insert")
    ch_client.execute("""
    ALTER TABLE citibyke_trip.dm_tripdata_count
    DELETE WHERE report_date in (select toDate(starttime) from citibyke_trip.stg_citibike_tripdata)
    """)

    log.info("Insert data")
    ch_client.execute("""
    INSERT INTO citibyke_trip.dm_tripdata_count(report_date, trip_count)
    SELECT toDate(starttime) as report_date, COUNT(*) as trip_count
    FROM citibyke_trip.stg_citibike_tripdata
    GROUP BY report_date
    """)


def dm_load_tripdata_avg():

    log.info("Clean up data before insert")
    ch_client.execute("""
    ALTER TABLE citibyke_trip.dm_tripdata_avg
    DELETE WHERE report_date in (select toDate(starttime) from citibyke_trip.stg_citibike_tripdata)
    """)

    log.info("Insert data")
    ch_client.execute("""
    INSERT INTO citibyke_trip.dm_tripdata_avg(report_date, avg_trip)
    SELECT toDate(starttime) as report_date, AVG(date_diff('minute', starttime, stoptime)) as avg_trip
    FROM citibyke_trip.stg_citibike_tripdata
    GROUP BY report_date
    """)



def dm_load_tripdata_by_gender():

    log.info("Clean up data before insert")
    ch_client.execute("""
    ALTER TABLE citibyke_trip.dm_tripdata_by_gender
    DELETE WHERE report_date in (select toDate(starttime) from citibyke_trip.stg_citibike_tripdata)
    """)

    log.info("Insert data")
    ch_client.execute("""
    INSERT INTO citibyke_trip.dm_tripdata_by_gender(report_date, gender, trip_count)
    SELECT toDate(starttime) as report_date, gender, COUNT(*) as trip_count
    FROM citibyke_trip.stg_citibike_tripdata
    GROUP BY report_date, gender
    """)
