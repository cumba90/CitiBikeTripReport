import boto3
from clickhouse_driver import connect
import config

s3 = boto3.resource(service_name="s3",
                    endpoint_url='https://storage.yandexcloud.net',
                    aws_access_key_id=config.common_settings['aws_access_key_id'],
                    aws_secret_access_key=config.common_settings['aws_secret_access_key']
                    )
bucket = s3.Bucket("out-citibike-tripreport")

#conn = connect(config.common_settings['ch_host'],
#               port=config.common_settings['ch_port'],
#               user=config.common_settings['ch_user'],
#               database=config.common_settings['ch_database'])

conn = connect('clickhouse://{}:@{}/{}'.format(
    config.common_settings['ch_user'],
    config.common_settings['ch_host'],
    config.common_settings['ch_database']
))

file_dest_path = '/tmp/files/'

def export_tripdata_count():

    table_name = 'dm_tripdata_count'
    columns = ['report_date', 'trip_count']

    file_name = table_name + ".csv"
    full_file_path = file_dest_path + file_name

    sql_str = "SELECT {} FROM {}".format(", ".join(columns), table_name)
    print("INFO: Start exporting....")

    print(sql_str)
    cur = conn.cursor()
    cur.execute(sql_str)
    res = cur.fetchall()

    f = open(full_file_path, 'w')
    f.write(columns[0] + ',' + columns[1] + "\n")
    for row in res:
        f.write(row[0].strftime("%Y-%m-%d") + ',' + str(row[1]) + "\n")
    f.close()

    bucket.upload_file(full_file_path, file_name)

    print("INFO: exporting ENDED")


def export_tripdata_avg():
    table_name = 'dm_tripdata_avg'
    columns = ['report_date', 'avg_trip']

    file_name = table_name + ".csv"
    full_file_path = file_dest_path + file_name

    sql_str = "SELECT {} FROM {}".format(", ".join(columns), table_name)
    print("INFO: Start exporting....")

    print(sql_str)
    cur = conn.cursor()
    cur.execute(sql_str)
    res = cur.fetchall()

    f = open(full_file_path, 'w')
    f.write(columns[0] + ',' + columns[1] + "\n")
    for row in res:
        f.write(row[0].strftime("%Y-%m-%d") + ',' + str(row[1]) + "\n")
    f.close()

    bucket.upload_file(full_file_path, file_name)

    print("INFO: exporting ENDED")

def export_dm_tripdata_by_gender():
    table_name = 'dm_tripdata_by_gender'
    columns = ['report_date', 'gender', 'trip_count']

    file_name = table_name + ".csv"
    full_file_path = file_dest_path + file_name

    sql_str = "SELECT {} FROM {}".format(", ".join(columns), table_name)
    print("INFO: Start exporting....")

    print(sql_str)
    cur = conn.cursor()
    cur.execute(sql_str)
    res = cur.fetchall()

    f = open(full_file_path, 'w')
    f.write(columns[0] + ',' + columns[1] + ',' + columns[2] + "\n")
    for row in res:
        f.write(row[0].strftime("%Y-%m-%d") + ',' + str(row[1]) + ',' + str(row[2]) + "\n")
    f.close()

    bucket.upload_file(full_file_path, file_name)

    print("INFO: exporting ENDED")
