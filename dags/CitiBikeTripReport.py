from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime
from programs import yaS3Sensor, Loader, yaS3Saver

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 27),
    "email": ["cumba90@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False
}


def set_last_load_date():
    processing_files_path = './dags/metadata/processing_files.txt'
    var = Variable.get("s3_last_catch_date")
    print("INFO: current catch date: ", var)
    last_load_date = datetime.strptime(var, "%Y-%m-%d %H:%M:%S.%f%z")

    f = open(processing_files_path, 'r')
    for row in f:
        load_date = datetime.strptime(row.split(';')[1].strip(), "%Y-%m-%d %H:%M:%S.%f%z")
        if load_date > last_load_date:
            last_load_date = load_date

    Variable.set("s3_last_catch_date", last_load_date)


with DAG("CitiBikeTripReport", default_args=default_args, catchup=False) as dag:
    start = DummyOperator(task_id="Start")
    rerun_dag = TriggerDagRunOperator(task_id="rerun_dag",
                                      trigger_dag_id="CitiBikeTripReport")

    check_new_files_task = PythonOperator(
        task_id="s3_check_new_files",
        python_callable=yaS3Sensor.check_new_files,
        dag=dag
    )

    load_stg_tripdata = PythonOperator(
        task_id="load_stg_tripdata",
        python_callable=Loader.stg_load_tripdata,
        dag=dag
    )

    dm_load_tripdata_cout = PythonOperator(
        task_id="dm_load_tripdata_cout",
        python_callable=Loader.dm_load_tripdata_cout,
        dag=dag
    )

    dm_load_tripdata_avg = PythonOperator(
        task_id="dm_load_tripdata_avg",
        python_callable=Loader.dm_load_tripdata_avg,
        dag=dag
    )

    dm_load_tripdata_by_gender = PythonOperator(
        task_id="dm_load_tripdata_by_gender",
        python_callable=Loader.dm_load_tripdata_by_gender,
        dag=dag
    )

    export_tripdata_count = PythonOperator(
        task_id="export_tripdata_count",
        python_callable=yaS3Saver.export_tripdata_count,
        dag=dag
    )

    export_tripdata_avg = PythonOperator(
        task_id="export_tripdata_avg",
        python_callable=yaS3Saver.export_tripdata_avg,
        dag=dag
    )

    export_dm_tripdata_by_gender = PythonOperator(
        task_id="export_dm_tripdata_by_gender",
        python_callable=yaS3Saver.export_dm_tripdata_by_gender,
        dag=dag
    )

    set_last_load_date = PythonOperator(
        task_id="set_last_load_date",
        python_callable=set_last_load_date,
        dag=dag
    )

    start >> check_new_files_task >> load_stg_tripdata

    load_stg_tripdata >> dm_load_tripdata_cout
    load_stg_tripdata >> dm_load_tripdata_avg
    load_stg_tripdata >> dm_load_tripdata_by_gender

    dm_load_tripdata_cout >> export_tripdata_count
    dm_load_tripdata_avg >> export_tripdata_avg
    dm_load_tripdata_by_gender >> export_dm_tripdata_by_gender

    export_tripdata_count >> set_last_load_date
    export_tripdata_avg >> set_last_load_date
    export_dm_tripdata_by_gender >> set_last_load_date

    set_last_load_date >> rerun_dag
