import boto3
import time
import os
from airflow.models import Variable
from datetime import datetime


def check_new_files():
    s3 = boto3.resource(service_name="s3",
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id="YCAJELW71kr3JilXH81T8meDC",
                        aws_secret_access_key="YCNQHqsYw0H2ot9Vq6DzsmHgpVZuUCjJ_jI6M9hs"
                        )

    bucket = s3.Bucket("in-citibike-tripdata")

    var = Variable.get("s3_last_catch_date",
                       default_var="1900-01-01 00:00:00.000000+00:00")
    last_load_date = datetime.strptime(var, "%Y-%m-%d %H:%M:%S.%f%z")
    print("last_load_date=", last_load_date)

    list_of_objects = bucket.objects.all()
    while list_of_objects == 0:
        time.sleep(10)
        list_of_objects = bucket.objects.all()

    processing_files_path = './dags/metadata/processing_files.txt'
    if os.path.exists(processing_files_path):
        os.remove(processing_files_path)

    for f in filter(lambda x: x.last_modified > last_load_date, list_of_objects):
        with open(processing_files_path, 'a') as pf:
            pf.write(f.key + ";" + f.last_modified.strftime("%Y-%m-%d %H:%M:%S.%f%z") + "\n")
