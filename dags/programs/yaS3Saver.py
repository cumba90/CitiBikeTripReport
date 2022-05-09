import boto3
from clickhouse_driver import connect
from dag_config import common_settings
import logging as log

s3 = boto3.resource(service_name="s3",
                    endpoint_url='https://storage.yandexcloud.net',
                    aws_access_key_id=common_settings['aws_access_key_id'],
                    aws_secret_access_key=common_settings['aws_secret_access_key']
                    )
bucket = s3.Bucket("out-citibike-tripreport")

conn = connect('clickhouse://{}:@{}/{}'.format(
    common_settings['ch_user'],
    common_settings['ch_host'],
    common_settings['ch_database']
))

file_dest_path = common_settings["file_landing_path"]


def start_export(sql_str, full_file_path, file_name, columns: list):
    import datetime
    log.info("Executing query {}".format(sql_str))
    cur = conn.cursor()
    cur.execute(sql_str)
    res = cur.fetchall()

    log.info("Exporting query result into {}".format(full_file_path))
    f = open(full_file_path, 'w')
    f.write(columns[0] + ',' + columns[1] + "\n")
    for row in res:
        try:
            f.write(row[0].strftime("%Y-%m-%d") + ',' + ','.join(list(map(str, row[1:]))) + "\n")
        except:
            log.error(row)
            f.close()
            raise
    f.close()

    bucket.upload_file(full_file_path, file_name)

    log.info("Export has ended")


def export_tripdata_count():
    table_name = 'dm_tripdata_count'
    columns = ['report_date', 'trip_count']

    file_name = table_name + ".csv"
    full_file_path = file_dest_path + file_name

    sql_str = "SELECT {} FROM {}".format(", ".join(columns), table_name)

    start_export(sql_str, full_file_path, file_name, columns)


def export_tripdata_avg():
    table_name = 'dm_tripdata_avg'
    columns = ['report_date', 'avg_trip']

    file_name = table_name + ".csv"
    full_file_path = file_dest_path + file_name

    sql_str = "SELECT {} FROM {}".format(", ".join(columns), table_name)

    start_export(sql_str, full_file_path, file_name, columns)


def export_dm_tripdata_by_gender():
    table_name = 'dm_tripdata_by_gender'
    columns = ['report_date', 'gender', 'trip_count']

    file_name = table_name + ".csv"
    full_file_path = file_dest_path + file_name

    sql_str = "SELECT {} FROM {}".format(", ".join(columns), table_name)

    start_export(sql_str, full_file_path, file_name, columns)
