# Структура функциональных каталогов и файлов:
<pre>
.
│   docker-compose-LocalExecutor.yml
│   LICENSE
│   README.md
│   __init__.py
│
├───config
│   │   airflow.cfg
│   │   dag_config.py
│   │   __init__.py
│
├───dags
│   │   CitiBikeTripReport.py
│   │   __init__.py
│   │
│   ├───metadata
│   │       processing_files.txt
│   │
│   ├───programs
│   │   │   Loader.py
│   │   │   yaS3Saver.py
│   │   │   yaS3Sensor.py
│   │   │   __init__.py
│
├───database
│   └───scripts
│       │   create_database.sql
│       │
│       └───db
│           ├───dm
│           │       dm_tripdata_avg.sql
│           │       dm_tripdata_by_gender.sql
│           │       dm_tripdata_count.sql
│           │
│           └───stg
│                   stg_citibyke_trips.sql
</pre>

1. config
    - airflow.cfg - настройки airflow
    - dag_config.py - содердит в себе параметры подключения к S3, БД приемнику, директории выгрузки отчетов и формирования метаданных.
2. dags
    - CitiBikeTripReport - Реализация оркестрации задач загрузки и формирования файлов отчетов
    - metadata - директория для сохранения файла "processing_files.txt" со списком не обработанных файлов источников + дата создания
    - programs - реализация функций чтения источника, ожидания новых файлов, загрузки данных в БД и выгрузки в s3
3. database
    - scripts/create_database.sql - скрипт создания БД
    - scripts/db/stg - скрипты создания промежуточных таблиц
    - scripts/db/dm - скрипты создания промежуточных витрин данных
4. docker-compose-LocalExecutor.yml
    - Файл разворачивания окружения airflow, clickhouse в docker

