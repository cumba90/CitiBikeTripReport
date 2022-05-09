import boto3
import time
import os
from airflow.models import Variable
from datetime import datetime
import logging as log
from dag_config import common_settings


def check_new_files():
    s3 = boto3.resource(service_name="s3",
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id=common_settings['aws_access_key_id'],
                        aws_secret_access_key=common_settings['aws_secret_access_key']
                        )

    bucket = s3.Bucket("in-citibike-tripdata")

    var = Variable.get("s3_last_catch_date",
                       default_var="1900-01-01 00:00:00.000000+00:00")
    last_load_date = datetime.strptime(var, "%Y-%m-%d %H:%M:%S.%f%z")
    log.info("last_load_date={}".format(last_load_date.strftime("%Y-%m-%d %H:%M:%S.%f%z")))

    list_of_objects = list(filter(lambda x: x.last_modified > last_load_date, bucket.objects.all()))
    log.info("Checking for new files...")
    while len(list(list_of_objects)) == 0:
        time.sleep(10)
        list_of_objects = list(filter(lambda x: x.last_modified > last_load_date, bucket.objects.all()))
    log.info("{} files have been found".format(len(list_of_objects)))

    processing_files_path = common_settings["processing_files_path"]
    log.info(os.getcwd())
    log.info("Replace metadata file {}".format(processing_files_path))
    if os.path.exists(processing_files_path):
        os.remove(processing_files_path)
        log.info("Processing file has been deleted")

    log.info("Writing new files and creation date into {}".format(processing_files_path))
    for row in list_of_objects:
        log.info(row.key + ";" + row.last_modified.strftime("%Y-%m-%d %H:%M:%S.%f%z") + "\n")
        with open(processing_files_path, 'a') as f:
            f.write(row.key + ";" + row.last_modified.strftime("%Y-%m-%d %H:%M:%S.%f%z") + "\n")
