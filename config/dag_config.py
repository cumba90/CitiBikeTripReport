from airflow.models import Variable

AWS_KEY_ID = Variable.get("aws_access_key_id")
AWS_ACCESS_KEY = Variable.get("aws_secret_access_key")

common_settings = dict(
    aws_access_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_ACCESS_KEY,
    ch_host='172.22.0.3',
    ch_port='9000',
    ch_user='citibyke',
    ch_database='citibyke_trip',
    processing_files_path='./dags/metadata/processing_files.txt',
    file_landing_path='/tmp/files/'
)