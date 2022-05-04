import os
import boto3
import zipfile
import pandas as pd
from clickhouse_driver import Client
import config

ch_client = Client(config.common_settings['ch_host'],
                   port=config.common_settings['ch_port'],
                   user=config.common_settings['ch_user'],
                   database=config.common_settings['ch_database'], settings={'use_numpy': True})

def stg_load_tripdata():


    s3 = boto3.resource(service_name="s3",
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id=config.common_settings['aws_access_key_id'],
                        aws_secret_access_key=config.common_settings['aws_secret_access_key']
                        )

    bucket = s3.Bucket("in-citibike-tripdata")

    ch_client.execute("TRUNCATE TABLE citibyke_trip.stg_citibike_tripdata")

    processing_files_path = './dags/metadata/processing_files.txt'

    f = open(processing_files_path, 'r')

    for obj in f:
        zip_file = obj.split(";")[0].strip()
        file_dest_path = '/tmp/files/'
        path_to_zip_file = file_dest_path + zip_file

        print("INFO: Downloading START")
        bucket.download_file(zip_file, path_to_zip_file)
        print("INFO: Downloading END")

        with zipfile.ZipFile(path_to_zip_file) as zf:
            for file in zf.namelist():
                if file.endswith(".csv") and '/' not in file:
                    zf.extract(file, file_dest_path)
                    if file[0:2] == "JC":
                        city = "JC"
                    else:
                        city = "NY"

                    df = pd.read_csv(file_dest_path + file)
                    df = df.assign(city=city)
                    print(list(df.columns))
                    print(file_dest_path + file, len(df.index))

                    print("INFO: Start INSERTING.... ")
                    try:
                        if 'ride_id' in list(df.columns):
                            df.columns = ["ride_id", "rideable_type", "starttime", "stoptime",
                                          "start_station_name", "start_station_id", "end_station_name", "end_station_id",
                                          "start_lat", "start_lng", "end_lat", "end_lng", "member_casual", "city"]

                            df = df.astype(str)
                            #df['start_lat'] = df2['start_lat'].astype(str)
                            #df2['start_lng'] = df2['start_lng'].astype(str)
                            #df2['end_lat'] = df2['end_lat'].astype(str)
                            #df2['end_lng'] = df2['end_lng'].astype(str)
                            print(df.info())
                            print(df)

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

                            #df2['birth_year'] = df2['birth_year'].fillna(-1).to_frame()
                            #df2['start_lat'] = df2['start_lat'].astype(str)
                            #df2['start_lng'] = df2['start_lng'].astype(str)
                            #df2['end_lat'] = df2['end_lat'].astype(str)
                            #df2['end_lng'] = df2['end_lng'].astype(str)
                            #df2['birth_year'] = df2['birth_year'].astype(int)
                            df = df.astype(str)
                            print(df.info())
                            print(df)

                            ch_client.insert_dataframe(
                                """INSERT INTO citibyke_trip.stg_citibike_tripdata(tripduration,starttime,stoptime,start_station_id,
                                start_station_name,start_lat,start_lng,end_station_id,end_station_name,end_lat,end_lng,
                                bikeid,usertype,birth_year,gender,city)
                                VALUES""", df
                            )
                        print("INFO: Inserting ENDED ")
                    except:
                        print("ERROR: INSERTING ERROR !!!!!!!!")
                        raise
                    os.remove(file_dest_path + file)

        os.remove(path_to_zip_file)

def dm_load_tripdata_cout():

    #ch_client.execute("""
    #DELETE FROM citibyke_trip.dm_tripdata_count
    #WHERE report_date in (select starttime from citibyke_trip.stg_citibike_tripdata)
    #""")

    ch_client.execute("""
    INSERT INTO citibyke_trip.dm_tripdata_count(report_date, trip_count)
    SELECT toDate(starttime) as report_date, COUNT(*) as trip_count
    FROM citibyke_trip.stg_citibike_tripdata
    GROUP BY report_date
    """)


def dm_load_tripdata_avg():

    #ch_client.execute("""
    #DELETE FROM citibyke_trip.dm_tripdata_avg
    #WHERE report_date in (select starttime from citibyke_trip.stg_citibike_tripdata)
    #""")

    ch_client.execute("""
    INSERT INTO citibyke_trip.dm_tripdata_avg(report_date, avg_trip)
    SELECT toDate(starttime) as report_date, AVG(date_diff('minute', starttime, stoptime)) as avg_trip
    FROM citibyke_trip.stg_citibike_tripdata
    GROUP BY report_date
    """)



def dm_load_tripdata_by_gender():

    #ch_client.execute("""
    #DELETE FROM citibyke_trip.dm_tripdata_by_gender
    #WHERE report_date in (select starttime from citibyke_trip.stg_citibike_tripdata)
    #""")

    ch_client.execute("""
    INSERT INTO citibyke_trip.dm_tripdata_by_gender(report_date, gender, trip_count)
    SELECT toDate(starttime) as report_date, gender, COUNT(*) as trip_count
    FROM citibyke_trip.stg_citibike_tripdata
    GROUP BY report_date, gender
    """)
