import datetime
import pandas as pd
from clickhouse_driver import Client


def test():
    from clickhouse_driver import connect
    conn = connect('clickhouse+native://citibyke:@localhost:9000/citibyke_trip')
    cursor = conn.cursor()
    # cursor.execute('SHOW TABLES')
    # res = cursor.fetchall()
    # print(res)

    # r = ('288', '9/1/2015 00:00:00', '9/1/2015 00:04:48', '263', 'Elizabeth St & Hester St', '40.71729', '-73.996375', '307', 'Canal St & Rutgers St', '40.71427487', '-73.98990025', '15479', 'Subscriber', '1989', '1', 'NY')
    # r1 = {"id": 1, "val": '288'}
    r1 = [[1, '288']]

    # ch_client = Client('localhost', port='9000', user='citibyke', database='citibyke_trip')
    cursor.execute("INSERT INTO test (id, val) VALUES", r1)
    # cursor.execute("INSERT INTO test (id, val) VALUES(1, 'XXX')")
    res = cursor.fetchall()
    print(res)


def test2():
    import zipfile
    processing_files_path = '../dags/metadata/processing_files.txt'
    f = open(processing_files_path, 'r')

    for obj in f:
        zip_file = obj.split(";")[0].strip()
        file_dest_path = 'C:\\citibike_tripdata\\'
        path_to_zip_file = file_dest_path + zip_file

        with zipfile.ZipFile(path_to_zip_file) as zf:
            # print(zf.namelist())
            for file in zf.namelist():
                if '/' not in file and file.endswith('.csv'):
                    print(file)


import boto3


def export_tripdata_count():
    from clickhouse_driver import Client, connect
    from datetime import datetime

    table_name = 'dm_tripdata_count'
    columns = ['report_date', 'trip_count']

    common_settings = dict(
        aws_access_key_id='YCAJELW71kr3JilXH81T8meDC',
        aws_secret_access_key='YCNQHqsYw0H2ot9Vq6DzsmHgpVZuUCjJ_jI6M9hs'
    )

    conn = connect(host='localhost', port='9000', user='citibyke', database='citibyke_trip')
    cur = conn.cursor()

    s3 = boto3.resource(service_name="s3",
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id=common_settings['aws_access_key_id'],
                        aws_secret_access_key=common_settings['aws_secret_access_key']
                        )

    bucket = s3.Bucket("out-citibike-tripreport")

    sql_str = "SELECT {} FROM {}".format(", ".join(columns), table_name)
    print("INFO: Start exporting....")

    print(sql_str)
    cur.execute(sql_str)
    res = cur.fetchall()
    file_dest_path = 'C:/tmp/'
    file_name = table_name + ".csv"
    full_file_path = file_dest_path + file_name
    # file_dest_path = '/tmp/files/'
    #print(res)
    f = open(full_file_path, 'w')
    f.write(columns[0] + ',' + columns[1] + "\n")
    for row in res:
        f.write(row[0].strftime("%Y-%m-%d") + ',' + str(row[1]) + "\n")
    f.close()

    bucket.upload_file(full_file_path, file_name)

    print("INFO: exporting ENDED")

def test3():
    r = (datetime.date(2015, 9, 1), 14, 10000)
    d = r[0].strftime("%Y-%m-%d")
    print(d, ',' + ','.join(list(map(str, r[1:]))))


test3()
